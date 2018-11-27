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
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.logging.Level;
import java.util.logging.Logger;
import static org.apache.http.HttpHeaders.USER_AGENT;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.util.EntityUtils;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.ParseException;
import org.umgc.umgcscraper.parser.TaxiAvailabilityParser;

/**
 *
 * @author samsudinj
 */
public class TaxiAvailabilityTimer implements Runnable{

    private final String url;
    private final String OutputFile;
    private final long loop;
    private final int SampleNo;
    public TaxiAvailabilityTimer(String url, String OutputFile, long loop, int SampleNo){
        this.url = url;
        this.OutputFile = OutputFile;
        this.loop = loop;
        this.SampleNo = SampleNo;
    }
    
    @Override
    public void run() {
        //throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
            HttpClient client = HttpClientBuilder.create().build();
            int n = 0;
            int i = 0;
            int bufferSize = 16 *1024;
            while(i < SampleNo){
                try {
                    String urlAddress = url + "?$skip=0";
                    HttpGet request = new HttpGet(urlAddress);    
                    request.addHeader("User-Agent", USER_AGENT);
                    request.addHeader("AccountKey","WT9HkF2lS6S7qfL1u6IOCA==");
                    request.addHeader("accept","application/json");
                    HttpResponse response = client.execute(request);
                    System.out.println("Response Code: " + response.getStatusLine().getStatusCode());
                    BufferedReader rd = new BufferedReader(new InputStreamReader(response.getEntity().getContent()), bufferSize);
                    StringBuilder result = new StringBuilder();
                    String line = ""  ;
                    while ((line = rd.readLine()) != null){
                        result.append(line);
                    }
                    /*Important or the response will be truncated at 8KB*/
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
                    i++;
                    Thread.sleep(loop * 1000);
                } catch (InterruptedException ex) {
                    Logger.getLogger(TaxiAvailabilityTimer.class.getName()).log(Level.SEVERE, null, ex);
                } catch (IOException ex) {
                    Logger.getLogger(TaxiAvailabilityTimer.class.getName()).log(Level.SEVERE, null, ex);
                } catch (ParseException ex) {
                    Logger.getLogger(TaxiAvailabilityTimer.class.getName()).log(Level.SEVERE, null, ex);
                } 
            }
        }
    
    }
