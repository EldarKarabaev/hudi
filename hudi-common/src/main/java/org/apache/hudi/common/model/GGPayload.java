package org.apache.hudi.common.model;

import org.apache.avro.data.Json;

import java.io.Serializable;
import java.util.Map;

public class GGPayload implements Serializable {

  private Object payload;
  private Map<String, Object> validityMap;

  public Object get(String name){
    return ((Map)payload).get(name);
  }

  public GGPayload(String json) {
    this.payload = Json.parseJson(json);
  }

  public String mapToString(Map<String,Object> map, String prefix){
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
    return "payload = " + mapToString((Map)payload, "  ");
  }

  public static void test() throws Exception {
    String jsonText = "{\"current_ts\":\"2021-07-17T17:32:15.098000\",\"primary_keys\":[\"FELD\",\"FINH\",\"FIR\",\"SPRA\"],\"pos\":\"00000000030001151082\",\"op_type\":\"I\",\"after\":{\"PBEZ\":\"CC-Corporate Client\",\"FELD\":\"AWKZ\",\"SPRA\":14,\"FINH\":\"021\",\"FIR\":1},\"op_ts\":\"2021-07-17 17:32:09.000773\",\"table\":\"COBSOFT.ALPRUEF\"}";
    GGPayload payload = new GGPayload(jsonText);
    System.out.println(payload.toDebugString());
  }

  public static void main(String[] args){
    try {
      test();
    } catch (Exception e){
      e.printStackTrace();
    }
  }
}
