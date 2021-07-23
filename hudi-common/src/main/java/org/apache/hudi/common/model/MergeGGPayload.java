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
import org.apache.avro.data.Json;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.IndexedRecord;
import org.apache.avro.util.Utf8;
import org.apache.hudi.avro.HoodieAvroUtils;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.exception.HoodieException;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

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

  public static final String GG_DATA_MAP_COLUMN_NAME = "_gg_data_map";
  public static final String GG_VALIDITY_MAP_COLUMN_NAME = "_gg_validity_map";
  public static final String OP_TS_COLUMN_NAME = "op_ts";

  public static final Utf8 OP_TS_COLUMN_NAME_UTF8 = new Utf8(OP_TS_COLUMN_NAME);

  private byte[] myAvroBytes;
  public byte[] getAvroBytes(){return myAvroBytes;}
  private String schemaString;
  //private Schema schema;

  public MergeGGPayload(GenericRecord record, Comparable orderingVal) {
    super(record, orderingVal);
    if(record == null){
      myAvroBytes = new byte[0];
    } else {
      // Check _gg_data_map column exists
      if(record.get(GG_DATA_MAP_COLUMN_NAME) == null){
        throw new HoodieException("Column not found: " + GG_DATA_MAP_COLUMN_NAME + " in " + record);
      }
      // Check _gg_validity_map column exists
      if(record.get(GG_VALIDITY_MAP_COLUMN_NAME) == null){
        throw new HoodieException("Column not found: " + GG_VALIDITY_MAP_COLUMN_NAME + " in " + record);
      }
      this.schemaString= record.getSchema().toString();
      // Create mutable record, modify and save in MyAvroBytes
      try {
        GenericRecord myRecord = HoodieAvroUtils.bytesToAvro(HoodieAvroUtils.avroToBytes(record), record.getSchema());
        // Open gg_data map
        Object ggDataMapObject = myRecord.get(GG_DATA_MAP_COLUMN_NAME);
        Map ggDataMap = (Map)ggDataMapObject;
        // Get op_ts as a string and Utf8
        Object opTsObject = ggDataMap.get(OP_TS_COLUMN_NAME_UTF8);
        if(opTsObject == null){
          throw new HoodieException("Field not found or null: " + GG_DATA_MAP_COLUMN_NAME + "." + OP_TS_COLUMN_NAME);
        }
        String opTsString = opTsObject.toString();
        Utf8 opTsUtf8 = new Utf8(opTsString);
        // Open existing validity_map or create new one
        Object validityMapObject = myRecord.get(GG_VALIDITY_MAP_COLUMN_NAME);
        Map validityMap;
        if(validityMapObject == null){
          validityMap = new HashMap();
        } else {
          validityMap = (Map) validityMapObject;
        }
        // Open "after"
        String afterString = ggDataMap.get(new Utf8("after")).toString();
        Map afterMap = (Map)Json.parseJson(afterString);
        // Actualize validityMap
        if(afterMap != null){
          for (Object key : afterMap.keySet()) {
            Utf8 fieldName = new Utf8(key.toString());
            Object currentValidityObject = validityMap.get(fieldName);
            String currentValidityString = null;
            if(currentValidityObject != null){
              currentValidityString = currentValidityObject.toString();
            }
            if(currentValidityString == null || currentValidityString.length() == 0 || opTsString.compareTo(currentValidityString) > 0){
              validityMap.put(key, opTsUtf8);
            }
          }
        }
        myRecord.put(GG_VALIDITY_MAP_COLUMN_NAME, validityMap);
        myAvroBytes = HoodieAvroUtils.avroToBytes(myRecord);
      } catch (Exception e){
        throw new HoodieException("Cannot initialize record:" + e.getMessage());
      }
    }
  }

  public MergeGGPayload(Option<GenericRecord> record) {
    this(record.isPresent() ? record.get() : null, 0); // natural order
  }

  /*
   * Merges data from another record into this one according to validityMaps of both
   */
  public void mergeAnotherRecord(GenericRecord another){
    if(another == null) {
      return;
    }

    // get another's validityMap
    Map anotherValidityMap = (Map)(another.get(GG_VALIDITY_MAP_COLUMN_NAME));
    // If it is not empty, then we merge more recent data from another
    if(anotherValidityMap != null && anotherValidityMap.keySet().size() > 0){
      // instantiate my GenericRecord
      String step = "";
      try {
        step = "01a";
        Schema.Parser parser = new Schema.Parser();
        Schema schema = parser.parse(this.schemaString);
        step="01b";
        GenericRecord myRecord = HoodieAvroUtils.bytesToAvro(myAvroBytes, schema);
        // get my validityMap
        step = "02";
        Map myValidityMap = (Map)(myRecord.get(GG_VALIDITY_MAP_COLUMN_NAME));
        for(Object fieldName: anotherValidityMap.keySet()){
          boolean needCopy = true;
          step = "03";
          String anotherValidityTs = anotherValidityMap.get(fieldName).toString();
          step = "04";
          if(myValidityMap.containsKey(fieldName)){
            step = "05";
            String myValidityTs = myValidityMap.get(fieldName).toString();
            step = "06";
            if(myValidityTs.compareTo(anotherValidityTs)>=0){
              needCopy = false;
            }
          }
          if(needCopy){
            // 1. Copy the data field value
            step = "07";
            myRecord.put(fieldName.toString().toLowerCase(), another.get(fieldName.toString().toLowerCase()));
            // 2. Copy the validityTs
            step = "08";
            myValidityMap.put(fieldName, anotherValidityMap.get(fieldName));
          }
        }
        step = "09";
        myAvroBytes = HoodieAvroUtils.avroToBytes(myRecord);
      } catch (Exception e){
        throw new HoodieException("Merge error 002[" + step + "]: " + e.getMessage(), e);
      }
    }

  }

  public void mergeAnotherPayload(MergeGGPayload another){
    try {
      Schema.Parser parser = new Schema.Parser();
      Schema schema = parser.parse(this.schemaString);
      GenericRecord anotherRecord = HoodieAvroUtils.bytesToAvro(another.getAvroBytes(), schema);
      mergeAnotherRecord(anotherRecord);
    } catch (Exception e){
      throw new HoodieException("Merge error 001: " + e.getMessage());
    }
  }

  @Override
  public MergeGGPayload preCombine(MergeGGPayload another) {
    mergeAnotherPayload(another);
    return this;
  }

  @Override
  public Option<IndexedRecord> combineAndGetUpdateValue(IndexedRecord currentValue, Schema schema) throws IOException {
    mergeAnotherRecord((GenericRecord)currentValue);
    return getInsertValue(schema);
  }

  @Override
  public Option<IndexedRecord> getInsertValue(Schema schema) throws IOException {
    if (recordBytes.length == 0) {
      return Option.empty();
    }
    IndexedRecord indexedRecord = HoodieAvroUtils.bytesToAvro(myAvroBytes, schema);
    if (isDeleteRecord((GenericRecord) indexedRecord)) {
      return Option.empty();
    } else {
      /*
      Object ggDataMapObject = ((GenericRecord) indexedRecord).get(GG_DATA_MAP_COLUMN_NAME);
      String ggDataMapContents = (ggDataMapObject==null?"NULL":"(" + ggDataMapObject.getClass().getCanonicalName() + ")");
      Map ggDataMap = (Map)ggDataMapObject;

      if(ggDataMap != null) {
        for (Object key : ggDataMap.keySet()) {
          Object value = ggDataMap.get(key);
          ggDataMapContents = ggDataMapContents
            + (ggDataMapContents.length() == 0?"":",")
            + (key==null?"NULL":"(" + key.getClass().getCanonicalName() + ")" + key.toString())
            + ": " + (value==null?"NULL":"(" + value.getClass().getCanonicalName() + ")" + value.toString())
          ;
        }

        ggDataMap.put("ggDataMapContents", ggDataMapContents);

        String afterString = ggDataMap.get(new Utf8("after")).toString();
        ggDataMap.put("afterString", "[" + afterString + "]");

        Map afterMap = (Map)Json.parseJson(afterString);
        String afterMapContents = "";
        if(afterMap == null){
          afterMapContents = "NULL";
        } else {
          for (Object key : afterMap.keySet()) {
            Object value = afterMap.get(key);
            afterMapContents = afterMapContents
              + (afterMapContents.length() == 0?"":",")
              + (key==null?"NULL":"(" + key.getClass().getCanonicalName() + ")" + key.toString())
              + ": " + (value==null?"NULL":"(" + value.getClass().getCanonicalName() + ")" + value.toString())
            ;
          }
        }
        ggDataMap.put("afterMapContents", afterMapContents);
      }
      //((GenericRecord) indexedRecord).put("feld","D10, recordBytes:" + recordBytes.length + ", myAvroBytes:" + myAvroBytes.length);
      */
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
