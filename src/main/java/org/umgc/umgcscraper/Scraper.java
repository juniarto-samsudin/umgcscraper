/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.umgc.umgcscraper;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileReader;
import java.io.FileWriter;

import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Iterator;
import static org.apache.http.HttpHeaders.USER_AGENT;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.HttpClientBuilder;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

/**
 *
 * @author samsudinj
 */
public class Scraper {

    /**
     * @param args the command line arguments
     * @throws java.io.IOException
     * @throws org.json.simple.parser.ParseException
     * @throws java.lang.InterruptedException
     */
    public static void main(String[] args) throws IOException, ParseException, InterruptedException {
        // TODO code application logic here
        
        JSONParser parser = new JSONParser();
        Object obj = parser.parse(new FileReader("scraper.conf"));
        JSONObject jsonObject = (JSONObject) obj;
        System.out.println(jsonObject);
        
        
        String url = (String)jsonObject.get("url");
        String OutputFile = (String)jsonObject.get("outputfile");
        System.out.println(OutputFile);
        long loop = (Long)jsonObject.get("loop");
        
        //String url = "https://api.data.gov.sg/v1/transport/taxi-availability";
    
        
        int n=0;
        while(true){
            HttpClient client = HttpClientBuilder.create().build();
            HttpGet request = new HttpGet(url);    
            request.addHeader("User-Agent", USER_AGENT);
            request.addHeader("AccountKey","WT9HkF2lS6S7qfL1u6IOCA==");
            request.addHeader("accept","application/json");
            HttpResponse response = client.execute(request);
            System.out.println("Response Code: " + response.getStatusLine().getStatusCode());
        
            BufferedReader rd = new BufferedReader(new InputStreamReader(response.getEntity().getContent()));
            StringBuilder result = new StringBuilder();
            String line = ""  ;
            while ((line = rd.readLine()) != null){
                result.append(line);
            }
            JSONParser ResultParser = new JSONParser();
            Object ResultObject  = ResultParser.parse(result.toString());
            JSONObject ResultJsonObject = (JSONObject)ResultObject;
            JSONArray FeaturesArray = (JSONArray)ResultJsonObject.get("features");
            Iterator<JSONObject> iterator = FeaturesArray.iterator();
            String TimeStamp="";
            while(iterator.hasNext()){
                JSONObject properties = (JSONObject) iterator.next().get("properties");
                if (properties != null){
                    TimeStamp = (String)properties.get("timestamp");
                    System.out.println(TimeStamp);
                }
            }
            //OutputFile = OutputFile + TimeStamp;
            String FileName = OutputFile + n;
            System.out.println(OutputFile);
            BufferedWriter bw = new BufferedWriter(new FileWriter(FileName));
            bw.write(result.toString());
            n++;
            Thread.sleep(loop * 1000);
        }   
    }
    
}
