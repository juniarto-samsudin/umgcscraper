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
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;
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
     * @throws java.util.concurrent.ExecutionException
     */
    
    
    public static void main(String[] args) throws IOException, ParseException, InterruptedException, ExecutionException {
        
        JSONParser parser = new JSONParser();
        Object obj = parser.parse(new FileReader("sb-scraper.conf"));
        JSONObject jsonObject = (JSONObject) obj;
        System.out.println(jsonObject);
        
        
        String url = (String)jsonObject.get("url1");
        String OutputFile = (String)jsonObject.get("outputfile");
        System.out.println(OutputFile);
        long loop = (Long)jsonObject.get("loop");
        
        int contSignal;
        
        ExecutorService HeartbeatExecutor = Executors.newFixedThreadPool(1);
        HeartbeatExecutor.submit(new HeartBeat());
        
        List<Integer> SkipList = new ArrayList<>();
        while(true){
            Timestamp ts = new Timestamp(System.currentTimeMillis());
            SimpleDateFormat sdf = new SimpleDateFormat("yyyy.MM.dd.HH.mm.ss");
            String sts = sdf.format(ts);
            System.out.println(sts);
            String arr_ts[] = sts.split("\\.");
            int array_size = arr_ts.length;
            System.out.println(array_size);
            System.out.println(arr_ts[array_size - 1]);
            System.out.println(arr_ts[array_size - 2]);
            int ss = Integer.parseInt(arr_ts[array_size - 1]);
            int mm = Integer.parseInt(arr_ts[array_size - 2]);
            System.out.println(mm);
            int begin = 0;
            int end = 4500 ;
            int minmod = mm%5;
            if ( ((minmod == 0) && (ss > 10))  
                ){
                contSignal = 1;
                System.out.println("DOWNLOAD!!!!");
                String DirPath = OutputFile + sts + "/";
                System.out.println(DirPath);
                Path path = Paths.get(DirPath);
                Files.createDirectories(path);
                while(contSignal == 1){
                    ExecutorService executor = Executors.newFixedThreadPool(10);
                    for(int i = begin; i <= end; i=i+500){
                        System.out.println(i);
                        Future<SpeedBandThreadResult> future = executor.submit(new SpeedBandCallable(url, i,  DirPath, contSignal));
                        SpeedBandThreadResult result = future.get();
                        contSignal = result.getContSignal();
                        if (result.getIsSorted() == true){
                            System.out.println("GOT SORTED FILENAME!!!");
                            System.out.println(result.getFileName());
                            SkipList.add(result.getSkip());
                            Thread.sleep(1000);
                        }
                    }
                    executor.shutdown();
                    executor.awaitTermination(5, TimeUnit.SECONDS);
                    begin = end + 500;
                    end = begin + 4500;
                    
                    System.out.println("Number of Sorted Files: " + SkipList.size());
                    for (int temp: SkipList){
                        System.out.println(temp);
                    }
                    Thread.sleep(5000);
                }
                if (SkipList.size() > 0){
                    ProcessAgain(SkipList, url, DirPath);
                }
            } else{
                System.out.println("NO!");
            }
            Thread.sleep(5000);
        }
        
        
    }

    private static void ProcessAgain(List<Integer> SkipList, String url, String DirPath) throws InterruptedException, ExecutionException{
        System.out.println("Process Again:" + SkipList.size());
        List<Integer> tempList = new ArrayList<>();
        ExecutorService executor = Executors.newFixedThreadPool(SkipList.size());
        for (int temp: SkipList){
            Future<SpeedBandThreadResult> future = executor.submit(new SpeedBandCallable(url, temp,  DirPath, 0));
            SpeedBandThreadResult result = future.get();
            if (result.getIsSorted() == true){
                tempList.add(result.getSkip());
            }
        }
        if (tempList.size() > 0){
                ProcessAgain(tempList, url, DirPath);
        }
        
    }
 
    @Override
    public void init(DaemonContext dc) throws DaemonInitException, Exception {
        System.out.println("Initializing....");
    }

    @Override
    public void start() throws Exception {
        System.out.println("Starting.....");
        main(null);
    }

    @Override
    public void stop() throws Exception {
        System.out.println("Stopping....");
    }

    @Override
    public void destroy() {
        System.out.println("Done.");
    }
    
}


class HeartBeat implements Runnable{
    @Override
    public void run() {
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
