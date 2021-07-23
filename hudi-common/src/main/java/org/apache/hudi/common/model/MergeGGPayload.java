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
import org.apache.hudi.avro.HoodieAvroUtils;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.exception.HoodieException;

import java.io.IOException;
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

  public static final String GG_DATA_MAP_COLUMN_NAME = "_gg_data_map";
  public static final String GG_VALIDITY_MAP_COLUMN_NAME = "_gg_validity_map";
  public static final String OP_TS_COLUMN_NAME = "op_ts";

  private byte[] myAvroBytes;

  public MergeGGPayload(GenericRecord record, Comparable orderingVal) {
    super(record, orderingVal);
    if(record == null){
      myAvroBytes = new byte[0];
    } else {
      if(record.get(GG_DATA_MAP_COLUMN_NAME) == null){
        throw new HoodieException("Column not found: " + GG_DATA_MAP_COLUMN_NAME + " in " + record);
      } else {
        if(((Map)record.get(GG_DATA_MAP_COLUMN_NAME)).get(OP_TS_COLUMN_NAME) == null){
          throw new HoodieException("Column not found: " + GG_DATA_MAP_COLUMN_NAME + "." + OP_TS_COLUMN_NAME + " in " + record);
        }
      }
      if(record.get(GG_VALIDITY_MAP_COLUMN_NAME) == null){
        throw new HoodieException("Column not found: " + GG_VALIDITY_MAP_COLUMN_NAME + " in " + record);
      }
      myAvroBytes = HoodieAvroUtils.avroToBytes(record);
    }
  }

  public MergeGGPayload(Option<GenericRecord> record) {
    this(record.isPresent() ? record.get() : null, 0); // natural order
  }

  @Override
  public MergeGGPayload preCombine(MergeGGPayload another) {
    // pick the payload with greatest ordering value
    if (another.orderingVal.compareTo(orderingVal) > 0) {
      return another;
    } else {
      return this;
    }
  }

  @Override
  public Option<IndexedRecord> combineAndGetUpdateValue(IndexedRecord currentValue, Schema schema) throws IOException {
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
      Map ggDataMap = (Map)(((GenericRecord) indexedRecord).get(GG_DATA_MAP_COLUMN_NAME));
      if(ggDataMap != null) {
        String ggDataMapContents = "";
        int ggDataNodesCounter = 0;
        for (Object key : ggDataMap.keySet()) {
          Object value = ggDataMap.get(key);
          ggDataMapContents = ggDataMapContents
            + (ggDataMapContents.length() == 0?"":",")
            + (key==null?"null":key.toString())
          //  + ": " + (value==null?"null":"(" + value.getClass().getCanonicalName() + ")" + value.toString())
          ;
          ggDataNodesCounter++;
        }
        ggDataMap.put("ggDataMapContents", "(" + ggDataMap.getClass().getCanonicalName() + ")[" + ggDataMapContents + "]");
        ggDataMap.put("ggDataMapNodes", ggDataNodesCounter);

        /*
        Object afterObject = ggDataMap.get("after");
        if(afterObject != null){
          ggDataMap.put("afterObjectClass", afterObject.getClass().getCanonicalName());
          if(Json.parseJson(afterObject.toString()) instanceof Map) {
            Map afterJson = (Map) Json.parseJson(afterObject.toString());
            String afterJsonContents = "";
            for(Object key: afterJson.keySet()){
              afterJsonContents = (afterJsonContents.length()==0?"":",") + key.toString()
                + "=" + (afterJson.get(key) == null?"null":afterJson.get(key));
            }
            afterJsonContents = "{" + afterJsonContents + "}";
            ggDataMap.put("afterJsonContents", afterJsonContents);
          } else {
            ggDataMap.put("afterObject", "not Map");
          }
        } else {
          ggDataMap.put("afterObject", "null");
        }
        */
      }

      Map valMap = (Map)(((GenericRecord) indexedRecord).get(GG_VALIDITY_MAP_COLUMN_NAME));
      if(valMap == null){
        ggDataMap.put("valMap","null");
      } else {
        valMap.put("Test key", "Test Value");
        ggDataMap.put("valMapClass",valMap.getClass().getCanonicalName());
        ggDataMap.put("valMap",valMap.toString());
      }


      ((GenericRecord) indexedRecord).put("feld","recordBytes:" + recordBytes.length + ", myAvroBytes:" + myAvroBytes.length);
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
