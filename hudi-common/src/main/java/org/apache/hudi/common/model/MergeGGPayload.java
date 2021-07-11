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

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.IndexedRecord;
import org.apache.hudi.avro.HoodieAvroUtils;
import org.apache.hudi.common.util.Option;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

/**
 * Default payload used for delta streamer.
 *
 * <ol>
 * <li> preCombine - Picks the latest delta record for a key, based on an ordering field;
 * <li> combineAndGetUpdateValue/getInsertValue - Simply overwrites storage with latest delta record
 * </ol>
 */
public class MergeGGPayload extends BaseAvroPayload
    implements HoodieRecordPayload<MergeGGPayload> {

  public static void sysout(String s){
    System.out.println("[MergeGGPayload: ]" + s);
  }

  public static final String GG_DATA_COLUMN_NAME = "sys_origin_json";

  // GG specific fields
  String ggDataJson;
  Map<String, Long> valuePositions;

  private void initValuePositions(GenericRecord record){
    Object ggDataJsonField = record.get(GG_DATA_COLUMN_NAME);
    if(ggDataJsonField == null){
      sysout("Field " + GG_DATA_COLUMN_NAME + " not found, GG-MERGE is OFF");
    } else {
      ggDataJson = ggDataJsonField.toString();
      sysout("GG-MERGE is ON, gg_data: " + ggDataJson);
      valuePositions = new TreeMap<>();
    }
  }

  public MergeGGPayload(GenericRecord record, Comparable orderingVal) {
    super(record, orderingVal);
    initValuePositions(record);
  }

  public MergeGGPayload(Option<GenericRecord> record) {
    this(record.isPresent() ? record.get() : null, 0); // natural order
  }

  @Override
  public MergeGGPayload preCombine(MergeGGPayload another) {
    // pick the payload with greatest ordering value
    sysout("Precombine. This    gg_data: " + this.ggDataJson);
    sysout("Precombine. Another gg_data: " + another.ggDataJson);
    if (another.orderingVal.compareTo(orderingVal) > 0) {
      return another;
    } else {
      return this;
    }
  }

  private String indexedRecordToString(IndexedRecord record){
    String res = "";
    Schema schema = record.getSchema();
    if(schema != null) {
      List<Schema.Field> fields = schema.getFields();
      for (Schema.Field field : fields) {
        res += (res.length() == 0 ? "IndexedRecord: " + field.toString() : "," + field.toString());
      }
    } else {
      sysout("Warning. IndexedRecord with null schema: " + record);
    }
    return res;
  }

  @Override
  public Option<IndexedRecord> combineAndGetUpdateValue(IndexedRecord currentValue, Schema schema) throws IOException {
    sysout("Combine. This   schema: " + schema.toString());
    sysout("Combine. CurVal schema: " + currentValue.getSchema().toString());
    sysout("Combine. This gg_data: " + this.ggDataJson);
    sysout("Combine. CurVal  data: " + indexedRecordToString(currentValue));
    return getInsertValue(schema);
  }

  @Override
  public Option<IndexedRecord> getInsertValue(Schema schema) throws IOException {
    if (recordBytes.length == 0) {
      return Option.empty();
    }
    IndexedRecord indexedRecord = HoodieAvroUtils.bytesToAvro(recordBytes, schema);
    sysout("GetInsert. " + indexedRecordToString(indexedRecord));
    if (isDeleteRecord((GenericRecord) indexedRecord)) {
      return Option.empty();
    } else {
      return Option.of(indexedRecord);
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
