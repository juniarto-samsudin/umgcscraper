/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.umgc.umgcscraper;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Iterator;
import java.util.logging.Level;
import java.util.logging.Logger;
import static org.apache.http.HttpHeaders.USER_AGENT;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.HttpClientBuilder;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import java.sql.Timestamp;
import java.util.Date;
import java.text.SimpleDateFormat;
import org.apache.http.util.EntityUtils;
import org.umgc.umgcscraper.parser.TaxiAvailabilityParser;

/**
 *
 * @author samsudinj
 */
public class TaxiAvailability implements Runnable {

    private final String url;
    private final String OutputFile;
    private final long loop;
    public TaxiAvailability(String url, String OutputFile, long loop){
        this.url = url;
        this.OutputFile = OutputFile;
        this.loop = loop;
    }
    @Override
    public void run() {
        //throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
        HttpClient client = HttpClientBuilder.create().build();
            HttpGet request = new HttpGet(url);    
            request.addHeader("User-Agent", USER_AGENT);
            request.addHeader("AccountKey","WT9HkF2lS6S7qfL1u6IOCA==");
            request.addHeader("accept","application/json");
            int n = 0;
            int bufferSize = 16 *1024;
            while(true){
                try {
                    HttpResponse response = client.execute(request);
                    System.out.println("Response Code: " + response.getStatusLine().getStatusCode());
                    BufferedReader rd = new BufferedReader(new InputStreamReader(response.getEntity().getContent()), bufferSize);
                    StringBuilder result = new StringBuilder();
                    String line = ""  ;
                    while ((line = rd.readLine()) != null){
                        result.append(line);
                    }
                    EntityUtils.consume(response.getEntity());
                    rd.close();
                    
                    TaxiAvailabilityParser theParser = new TaxiAvailabilityParser(result.toString());
                    System.out.println("Number of Position: " + theParser.getNumberOfPosition());
                    /*
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
                    */
                    
                    Timestamp ts = new Timestamp(System.currentTimeMillis());
                    SimpleDateFormat sdf = new SimpleDateFormat("yyyy.MM.dd.HH.mm.SS");
                    
                    String TimeStamp = sdf.format(ts);
                    String FileName = OutputFile + TimeStamp;
                    System.out.println(OutputFile);
                    BufferedWriter bw = new BufferedWriter(new FileWriter(FileName), 16*1024);
                    bw.write(result.toString());
                    bw.close();
                    n++;
                    Thread.sleep(loop * 1000);
                } catch (IOException | InterruptedException ex) {
                    Logger.getLogger(Downloader.class.getName()).log(Level.SEVERE, null, ex);
                } catch (ParseException ex) {
                Logger.getLogger(TaxiAvailability.class.getName()).log(Level.SEVERE, null, ex);
            }
        }
    
    
    
    
    }
    
}
