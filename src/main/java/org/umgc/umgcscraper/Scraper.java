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
import java.util.Properties;
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

import org.apache.commons.daemon.Daemon;
import org.apache.commons.daemon.DaemonContext;
import org.apache.commons.daemon.DaemonInitException;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
/**
 *
 * @author samsudinj
 */
public class Scraper implements Daemon{
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
        
        
        String url = (String)jsonObject.get("url1");
        String OutputFile = (String)jsonObject.get("outputfile");
        System.out.println(OutputFile);
        long loop = (Long)jsonObject.get("loop");
        
        Thread HeartBeatThread = new Thread(new HeartBeat());
        //Thread DownloaderThread = new Thread(new Downloader(url, OutputFile, loop));
        HeartBeatThread.start();
        //DownloaderThread.start();
        Thread TaxiAvailability = new Thread(new TaxiAvailability(url, OutputFile, loop));
        TaxiAvailability.start();
       
        
        //String url = "https://api.data.gov.sg/v1/transport/taxi-availability";
    
        /*
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
            String FileName = OutputFile + TimeStamp;
            System.out.println(OutputFile);
            BufferedWriter bw = new BufferedWriter(new FileWriter(FileName));
            bw.write(result.toString());
            n++;
            Thread.sleep(loop * 1000);
        }*/   
    }

    @Override
    public void init(DaemonContext dc) throws DaemonInitException, Exception {
        //throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
        System.out.println("Initializing....");
    }

    @Override
    public void start() throws Exception {
        //throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
        System.out.println("Starting.....");
        main(null);
    }

    @Override
    public void stop() throws Exception {
        //throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
        System.out.println("Stopping....");
    }

    @Override
    public void destroy() {
        //throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
        System.out.println("Done.");
    }
    
}
class Downloader implements Runnable{
    String url;
    String OutputFile;
    long loop;
    
    public Downloader(String url, String OutputFile, long loop){
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
            while(true){
                try {
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
                    String FileName = OutputFile + TimeStamp;
                    System.out.println(OutputFile);
                    BufferedWriter bw = new BufferedWriter(new FileWriter(FileName));
                    bw.write(result.toString());
                    n++;
                    Thread.sleep(loop * 1000);
                } catch (IOException | ParseException | InterruptedException ex) {
                    Logger.getLogger(Downloader.class.getName()).log(Level.SEVERE, null, ex);
                }
        }
    }
 }
    




class HeartBeat implements Runnable{
    @Override
    public void run() {
        //throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
        String key = "Key1";
        String value = "Value1";
        String topicName = "HeartBeat";
        Properties props = new Properties();
        props.put("bootstrap.servers", "172.20.116.17:9092,172.20.116.18:9092,172.20.116.19:9092");
        props.put("key.serializer","org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer","org.apache.kafka.common.serialization.StringSerializer");
        Producer<String, String> producer = new KafkaProducer<>(props);
        ProducerRecord<String, String> record = new ProducerRecord<>(topicName, key, value);
        
        while(true){
            System.out.println("Sending heartbeat....");
            producer.send(record);
            try {
                Thread.sleep(5000);
            } catch (InterruptedException ex) {
                Logger.getLogger(HeartBeat.class.getName()).log(Level.SEVERE, null, ex);
            }
        }
    }    
}
