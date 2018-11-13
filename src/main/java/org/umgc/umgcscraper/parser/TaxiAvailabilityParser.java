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
public class TaxiAvailabilityParser {

    private final String jsontext;
    private Object parseObject;
    private JSONObject jsonObject;
    private JSONArray positionArray;
    public TaxiAvailabilityParser(String jsontext) throws ParseException{
        this.jsontext = jsontext;
        processJson();
        
    }
    
    private void processJson() throws ParseException{
        JSONParser jsonParser = new JSONParser();
        parseObject = jsonParser.parse(jsontext);
        jsonObject = (JSONObject)parseObject;
        positionArray = (JSONArray)jsonObject.get("value");
    }
    
    public int getNumberOfPosition(){
        return positionArray.size() ;
    }
}
