package org.apache.hudi.common.model;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.serializers.JavaSerializer;
import org.apache.avro.data.Json;
import org.apache.avro.generic.GenericRecord;
import org.apache.hudi.exception.HoodieException;

import java.io.Serializable;
import java.util.Map;
import java.util.TreeMap;

public class GGPayload implements Serializable {
  static {
    new Kryo().register(GGPayload.class, new JavaSerializer());
  }

  private GenericRecord record;
  private TreeMap<String, Object> ggData; // GG event: (before,after,op_type,...)
  private TreeMap<String, Object> before, after;
  private TreeMap<String, String> validityMap; // Field name -> comparable value

  public GenericRecord getRecord(){return this.record;}
  public TreeMap<String, Object> getGgData() {return this.ggData;}
  public TreeMap<String, Object> getBefore() {return this.before;}
  public TreeMap<String, Object> getAfter() {return this.after;}

  public TreeMap<String, String> getValidityMap() {return this.validityMap;}

  public Object getValue(String name){
    return ggData.get(name);
  }

  public GGPayload(){}

  public GGPayload(String ggDataJsonString, String validityMapJsonString, GenericRecord record) {
    this.record = record;
    this.ggData = new TreeMap<>();
    this.ggData.putAll((Map)Json.parseJson(ggDataJsonString));

    this.validityMap = (TreeMap)Json.parseJson(validityMapJsonString);
    if(this.validityMap == null){
      this.validityMap = new TreeMap<>();
      if(this.ggData.get("before") != null) {
        this.before = new TreeMap<>();
        this.before.putAll((Map) this.ggData.get("before"));
      }
      if(this.ggData.get("after") != null) {
        this.after = new TreeMap<>();
        this.after.putAll((Map) this.ggData.get("after"));
      }
      if(this.after != null) {
        String pos = this.ggData.get("pos").toString();
        for(String fieldName: this.after.keySet()){
          this.validityMap.put(fieldName, pos);
        }
      } else {
        this.after = new TreeMap<>();
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
    TreeMap<String, Object> anotherGgData = another.getGgData();
    TreeMap<String, String> anotherValidityMap = another.getValidityMap();
    if(anotherGgData != null && anotherValidityMap != null) {
      for(String anotherFieldName: anotherValidityMap.keySet()){
        String anotherFieldValidityValue = anotherValidityMap.get(anotherFieldName);
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
      //throw new HoodieException("Another has empty gg_data or validity_map: " + another.toDebugString());
      throw new HoodieException("Another has empty gg_data or validity_map: " + another.getRecord().toString());
    }
    return updatedFlag;
  }

  /*


  public String mapToString(TreeMap map, String prefix){
    if(map == null)
      return "null";
    else {
      StringBuilder res = new StringBuilder("");
      map.forEach((name,object) -> {
        if (object instanceof TreeMap) res.append("\n" + prefix + name + " = (" + mapToString((TreeMap)object, prefix + "  "));
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

    Kryo kryo = new Kryo();
    GGPayload copy3 = kryo.copy(payload3);
    System.out.println("KRYO copy:");
    System.out.println(copy3.toDebugString());
  }

  public static void main(String[] args){
    try {
      test();
    } catch (Exception e){
      e.printStackTrace();
    }
  }
  */
}
