/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.umgc.umgcscraper.parser;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

/**
 *
 * @author samsudinj
 */
public class SpeedBandParser {
    private final String jsontext;
    private Object parseObject;
    private JSONObject jsonObject;
    private JSONArray linkArray;
    private String x;
    public SpeedBandParser(String jsontext) throws ParseException{
        this.jsontext = jsontext;
        processJson();
        
    }
    
    private void processJson() throws ParseException{
        JSONParser jsonParser = new JSONParser();
        parseObject = jsonParser.parse(jsontext);
        jsonObject = (JSONObject)parseObject;
        linkArray = (JSONArray)jsonObject.get("value");
    }
    
    public int getNumberOfLink(){
        return linkArray.size();
    } 
    
    public String printLinkArray(){
       StringBuilder result = new StringBuilder();
       for(int i = 0; i < linkArray.size(); i++){
           JSONObject link = (JSONObject)linkArray.get(i);
           result.append(link.get("LinkID"));
           result.append(":");
       }
       return result.toString();
    }
    
    public boolean isSorted(){
        if (linkArray.isEmpty()){
            return false;
        }
        for(int i = 0; i < linkArray.size()-1; i++){
            JSONObject link = (JSONObject)linkArray.get(i);
            JSONObject linkNext = (JSONObject)linkArray.get(i+1);
            if ( Integer.parseInt(link.get("LinkID").toString()) > Integer.parseInt(linkNext.get("LinkID").toString())){
                return false;
            }
        }
        return true;
    }
    
    
}
