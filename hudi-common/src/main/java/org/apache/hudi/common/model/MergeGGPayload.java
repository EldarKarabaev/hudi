/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hudi.common.model;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.serializers.JavaSerializer;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.IndexedRecord;
import org.apache.hudi.avro.HoodieAvroUtils;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.exception.HoodieException;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

/**
 * GG partial-update payload with validity map.
 * Assumes dataset to have the following columns:
 * <ol>
 *   <li>gg_data - original GG message data as a string</li>
 *   <li>gg_validity_map - empty String, to be used and filled internally during partial update</li>
 *   <li>meta - structure with technical fields</li>
 *   <li>RECORD_KEY - standard record key</li>
 *   <li>PRECOMBINE_KEY - standard precombine key (sort key)</li>
 * </ol>
 *
 * <ol>
 * <li> preCombine - Picks the latest delta record for a key, based on an ordering field;
 * <li> combineAndGetUpdateValue/getInsertValue - Simply overwrites storage with latest delta record
 * </ol>
 */
public class MergeGGPayload extends BaseAvroPayload
    implements HoodieRecordPayload<MergeGGPayload> {


  public static final String GG_DATA_COLUMN_NAME = "gg_data"; // original GG data represented as a String
  public static final String VALIDITY_MAP_COLUMN_NAME = "gg_validity_map"; // resulting validity map represented as String

  // This GenericRecord and bytes
  private GenericRecord myRecord;
  private byte[] updatedRecordBytes;

  // ValidityMap
  String validityDataJson;

  // GG specific fields
  private String ggDataJson;
  private GGPayload ggPayload;
  public GGPayload getGgPayload() {return this.ggPayload;}

  /*
   * Life-Cycle 1: initialization from a GenericRecord
   */
  public MergeGGPayload(GenericRecord record, Comparable orderingVal) {
    super(record, orderingVal);
    myRecord = record;
    initFromRecord(record);
    this.updatedRecordBytes = Arrays.copyOf(this.recordBytes, this.recordBytes.length);
  }
  public MergeGGPayload(Option<GenericRecord> record) {
    this(record.isPresent() ? record.get() : null, 0); // natural order
  }
  private void initFromRecord(GenericRecord record) {
    this.ggDataJson = record.get(GG_DATA_COLUMN_NAME).toString();
    this.validityDataJson = record.get(VALIDITY_MAP_COLUMN_NAME).toString();
    if(this.ggDataJson == null){
      throw new HoodieException("GG Column not found [" + GG_DATA_COLUMN_NAME + "] in record: " + record);
    }
    if(this.validityDataJson == null){
      throw new HoodieException("VAL_MAP Column not found [" + VALIDITY_MAP_COLUMN_NAME + "] in record: " + record);
    }

    this.ggPayload = new GGPayload(this.ggDataJson, this.validityDataJson, this.myRecord);
  }

  private void updateRecordBytes() {
    this.updatedRecordBytes = myRecord!= null ? HoodieAvroUtils.avroToBytes(myRecord) : new byte[0];
  }

  /*
   * Life-Cycle 2: single comparison
   * New feature: the winner merges missing fields from the looser and updates his validityMap accordingly
   */
  @Override
  public MergeGGPayload preCombine(MergeGGPayload another) {
    if (another.orderingVal.compareTo(orderingVal) > 0) {
      another.mergeValuesFromAnother(this);
      return another;
    } else {
      this.mergeValuesFromAnother(another);
      return this;
    }
  }
  public MergeGGPayload mergeValuesFromAnother(MergeGGPayload another){
    if(this.ggPayload.mergeAnother(another.getGgPayload())){
      this.updateRecordBytes();
    }
    return this;
  }

  /*
   * Life-Cycle 3: Combine me with existing record <currentValue>, return the result as an indexed record
   */
  @Override
  public Option<IndexedRecord> combineAndGetUpdateValue(IndexedRecord currentValue, Schema schema) throws IOException {
    if(currentValue != null) {
      //   convert currentValue into MergeGGPayload
      byte[] currentValueBytes = HoodieAvroUtils.indexedRecordToBytes(currentValue);
      GenericRecord currentGenericRecord = HoodieAvroUtils.bytesToAvro(currentValueBytes, schema);
      MergeGGPayload currentMergeGGPayload = new MergeGGPayload(currentGenericRecord, 0);
      //   merge data from currentValue into me
      this.mergeValuesFromAnother(currentMergeGGPayload);
    }
    return getInsertValue(schema);
  }

  /*
   * Life-Cycle 4: Return final variant of the record to be inserted
   */
  @Override
  public Option<IndexedRecord> getInsertValue(Schema schema) throws IOException {
    //   update value of my "gg_data" field
    this.myRecord.put(GG_DATA_COLUMN_NAME, this.ggPayload.getGgDataAsJsonString());
    //   update value of my "gg_validity_map" field
    this.myRecord.put(VALIDITY_MAP_COLUMN_NAME, this.ggPayload.getValidityMapAsJsonString());
    this.updateRecordBytes();

    if (this.updatedRecordBytes.length == 0) {
      return Option.empty();
    }
    IndexedRecord indexedRecord = HoodieAvroUtils.bytesToAvro(this.updatedRecordBytes, schema);
    if (isDeleteRecord((GenericRecord) indexedRecord)) {
      return Option.empty();
    } else {
      //   return copy of my record
      return Option.of(HoodieAvroUtils.bytesToAvro(this.updatedRecordBytes, schema));
    }
  }

  /**
   * @param genericRecord instance of {@link GenericRecord} of interest.
   * @returns {@code true} if record represents a delete record. {@code false} otherwise.
   */
  protected boolean isDeleteRecord(GenericRecord genericRecord) {
    final String isDeleteKey = "_hoodie_is_deleted";
    // Modify to be compatible with new version Avro.
    // The new version Avro throws for GenericRecord.get if the field name
    // does not exist in the schema.
    if (genericRecord.getSchema().getField(isDeleteKey) == null) {
      return false;
    }
    Object deleteMarker = genericRecord.get(isDeleteKey);
    return (deleteMarker instanceof Boolean && (boolean) deleteMarker);
  }

  /**
   * Return true if value equals defaultValue otherwise false.
   */
  public Boolean overwriteField(Object value, Object defaultValue) {
    return defaultValue == null ? value == null : defaultValue.toString().equals(String.valueOf(value));
  }
}
