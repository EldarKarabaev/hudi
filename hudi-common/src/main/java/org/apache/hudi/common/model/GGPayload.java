package org.apache.hudi.common.model;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.serializers.JavaSerializer;
import org.apache.avro.data.Json;
import org.apache.avro.generic.GenericRecord;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.io.Serializable;
import java.util.LinkedHashMap;
import java.util.Map;

public class GGPayload implements Serializable {
  static {
    new Kryo().register(GGPayload.class, new JavaSerializer());
  }

  private static final Logger LOG = LogManager.getLogger(GGPayload.class);

  private GenericRecord record;
  private Map<String, Object> ggData; // GG event: (before,after,op_type,...)
  private Map<String, Object> before, after;
  private Map<String, Comparable> validityMap; // Field name -> comparable value

  public GenericRecord getRecord(){return this.record;}
  public Map<String, Object> getGgData() {return this.ggData;}
  public Map<String, Object> getBefore() {return this.before;}
  public Map<String, Object> getAfter() {return this.after;}

  public Map<String, Comparable> getValidityMap() {return this.validityMap;}

  public Object getValue(String name){
    return ggData.get(name);
  }

  public GGPayload(String ggDataJsonString, String validityMapJsonString, GenericRecord record) {
    this.record = record;
    this.ggData = (Map)Json.parseJson(ggDataJsonString);
    this.validityMap = (Map)Json.parseJson(validityMapJsonString);
    if(this.validityMap == null){
      this.validityMap = new LinkedHashMap<>();
      this.before = (Map) this.ggData.get("before");
      this.after = (Map)this.ggData.get("after");
      if(this.after != null) {
        String pos = this.ggData.get("pos").toString();
        for(String fieldName: this.after.keySet()){
          this.validityMap.put(fieldName, pos);
        }
      } else {
        this.after = new LinkedHashMap<>();
        this.ggData.put("after", this.after);
      }
    }
  }

  public String getGgDataAsJsonString(){
    return Json.toString(this.ggData);
  }
  public String getValidityMapAsJsonString(){
    return Json.toString(this.validityMap);
  }

  /*
   * Merges data and validityMap from another GGPayload object
   */
  public boolean mergeAnother(GGPayload another){
    boolean updatedFlag = false;
    Map<String, Object> anotherGgData = another.getGgData();
    Map<String, Comparable> anotherValidityMap = another.getValidityMap();
    if(anotherGgData != null && anotherValidityMap != null) {
      for(String anotherFieldName: anotherValidityMap.keySet()){
        Comparable anotherFieldValidityValue = anotherValidityMap.get(anotherFieldName);
        if(!this.validityMap.containsKey(anotherFieldName)
          || (this.validityMap.containsKey(anotherFieldName)
          && this.validityMap.get(anotherFieldName).compareTo(anotherFieldValidityValue) < 0)
        ){
          // Overwrite needed
          // 1. Replace field value
          this.after.put(anotherFieldName, another.getAfter().get(anotherFieldName));
          if(this.record != null){
            this.record.put(anotherFieldName, another.getRecord().get(anotherFieldName));
          }
          // 2. Actualize validityMap
          this.validityMap.put(anotherFieldName, anotherFieldValidityValue);
          updatedFlag = true;
        }
      }

    } else {
      LOG.warn("Another has empty gg_data or validity_map: " + another.toDebugString());
    }
    return updatedFlag;
  }

  public String mapToString(Map map, String prefix){
    if(map == null)
      return "null";
    else {
      StringBuilder res = new StringBuilder("");
      map.forEach((name,object) -> {
        if (object instanceof Map) res.append("\n" + prefix + name + " = (" + mapToString((Map)object, prefix + "  "));
        else res.append("\n" + prefix + name + " = (" + object.getClass().getName() + ")" + object.toString());
      });
      return res.toString();
    }
  }

  public String toDebugString(){
    return "ggData = " + mapToString(ggData, "  ")
         + "\nvalidity_map = " + mapToString(validityMap, "  ")
      ;
  }

  public static void test() throws Exception {
    String jsonText1 = "{\"current_ts\":\"2021-07-17T17:32:15.098000\",\"primary_keys\":[\"FELD\",\"FINH\",\"FIR\",\"SPRA\"],\"pos\":\"00000000030001151082\",\"op_type\":\"I\",\"after\":{\"PBEZ\":\"CC-Corporate Client\",\"FELD\":\"AWKZ\",\"SPRA\":14,\"FINH\":\"021\",\"FIR\":1, \"COMMENT1\":\"Initial\", \"COMMENT2\":\"Initial\", \"COMMENT3\":\"Initial\"},\"op_ts\":\"2021-07-17 17:32:09.000773\",\"table\":\"COBSOFT.ALPRUEF\"}"
         , jsonText2 = "{\"current_ts\":\"2021-07-17T19:32:15.098000\",\"primary_keys\":[\"FELD\",\"FINH\",\"FIR\",\"SPRA\"],\"pos\":\"00000000050001151082\",\"op_type\":\"U\",\"after\":{\"PBEZ\":\"CC-Corporate Client++\",\"FELD\":\"AWKZ\",\"SPRA\":14,\"FINH\":\"021\",\"FIR\":1, \"COMMENT2\":\"Updating PBEZ\"},\"op_ts\":\"2021-07-17 19:32:09.000773\",\"table\":\"COBSOFT.ALPRUEF\"}"
         , jsonText3 = "{\"current_ts\":\"2021-07-17T18:32:15.098000\",\"primary_keys\":[\"FELD\",\"FINH\",\"FIR\",\"SPRA\"],\"pos\":\"00000000040001151082\",\"op_type\":\"U\",\"after\":{\"PBEZ\":\"CC-Corporate Client+\",\"FELD\":\"AWKZ\",\"SPRA\":14,\"FINH\":\"021\",\"FIR\":1, \"COMMENT3\":\"Updating PBEZ\"},\"op_ts\":\"2021-07-17 18:32:09.000773\",\"table\":\"COBSOFT.ALPRUEF\"}"
      ;
    GGPayload payload1 = new GGPayload(jsonText1, "", null)
            , payload2 = new GGPayload(jsonText2, "", null)
            , payload3 = new GGPayload(jsonText3, "", null)
      ;
    System.out.println(payload1.toDebugString());
    System.out.println(payload2.toDebugString());
    System.out.println(payload3.toDebugString());
  }

  public static void main(String[] args){
    try {
      test();
    } catch (Exception e){
      e.printStackTrace();
    }
  }
}
