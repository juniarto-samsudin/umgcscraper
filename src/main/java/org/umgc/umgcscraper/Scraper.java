/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.umgc.umgcscraper;

import astar.ihpc.umgc.umgcscraper.util.PaginationRequest;
import astar.ihpc.umgc.umgcscraper.util.PaginationResult;
import astar.ihpc.umgc.umgcscraper.util.RealTimeStepper;
import astar.ihpc.umgc.umgcscraper.util.ScraperClient;
import astar.ihpc.umgc.umgcscraper.util.ScraperResult;
import astar.ihpc.umgc.umgcscraper.util.ScraperUtil;
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
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.function.BiPredicate;
import java.util.function.Function;
import java.util.function.IntFunction;
import java.util.function.Predicate;
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
import org.asynchttpclient.Request;
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
        Object obj = parser.parse(new FileReader("ta-scraper.conf"));
        JSONObject jsonObject = (JSONObject) obj;
        System.out.println(jsonObject);
        
        String OutputFile = (String)jsonObject.get("outputfile");
        System.out.println(OutputFile);
        long loop = (Long)jsonObject.get("loop");
        
        String accountKey = (String)jsonObject.get("accountkey");
        
        final ScheduledThreadPoolExecutor scheduler = new ScheduledThreadPoolExecutor(1);
        ScraperClient client = ScraperUtil.createScraperClient(8, 50);
        
        final long startTimeMillis = ScraperUtil.convertToTimeMillis(2018, 1, 1, 0, 0, 0, ZoneId.of("Asia/Singapore"));
        final long timeStepMillis = 60_000;
        final long maxOvershootMillis = 20_000;
        final long maxRandomDelayMillis = 5_000;
        
        final long maxRuntimeMillis = 35_000; 
        
        final RealTimeStepper stepper = ScraperUtil.createRealTimeStepper(startTimeMillis, timeStepMillis, maxOvershootMillis, maxRandomDelayMillis);
        final DateTimeFormatter dateTimeFmt = DateTimeFormatter.ISO_LOCAL_DATE_TIME;
        
        final IntFunction<Request> pageCreateFunction = pageNo->{
		String url = String.format("http://datamall2.mytransport.sg/ltaodataservice/Taxi-Availability?$skip=%d", pageNo * 500);
		Request req = ScraperUtil.createRequestBuilder().setUrl(url).setHeader("AccountKey", accountKey).build();
		return req;
	};
        
        final Function<Request, CompletableFuture<ScraperResult<TaxiAvailabilityDocumentJson>>> pageRequestFunction = req -> {
		return client.requestJson(req, TaxiAvailabilityDocumentJson.class);
	};
        
        final int batchSize = client.getMaxConcurrentRequests() * 2;
        
        final Predicate<ScraperResult<TaxiAvailabilityDocumentJson>> lastPageTest = (res)->res.getResponseData().getValue().size() < 500;
        
        final Predicate<ScraperResult<TaxiAvailabilityDocumentJson>> emptyPageTest = (res)->res.getResponseData().getValue().isEmpty();
        
        final Predicate<ScraperResult<TaxiAvailabilityDocumentJson>> goodResultTest = (res)->true;
        
        final BiPredicate<Request, Throwable> retryOnErrorTest = (req, t)->true;
        
        final int maxRetries = 3;
        
        final int retryMinDelayMillis = 1000;
        final int retryMaxDelayMillis = 3000;
        
        final PaginationRequest<TaxiAvailabilityDocumentJson> preq = new PaginationRequest<>(
		pageCreateFunction, pageRequestFunction, scheduler, batchSize, 
		lastPageTest, emptyPageTest, goodResultTest, retryOnErrorTest, maxRetries, retryMinDelayMillis, retryMaxDelayMillis
	);
        
        while (true){
            try{
                    System.out.println("Waiting for next step...");
                    stepper.nextStep(); //Sleep until the next step.
                    System.out.println("Step triggered: " + dateTimeFmt.format(LocalDateTime.now()));
            
                    long deadlineMillis = stepper.calcCurrentStepMillis() + maxRuntimeMillis;
                    
                    CompletableFuture<PaginationResult<TaxiAvailabilityDocumentJson>> future = preq.requestPages(deadlineMillis);
                    PaginationResult<TaxiAvailabilityDocumentJson> pres = future.join();
                    int size = pres.size(); //Total number of pages returned.
                    List<TaxiAvailabilityDocumentJson> allDocs = pres.getResponseData(); //Get all the response data in a list.
                    
                    String DirPath = createDirectory(OutputFile);
                    for (int i = 0; i < size; i++) {
			int pageNo = pres.getPageNumber(i); //Usually pageNo==i, but sometimes you request specific pages only.
			TaxiAvailabilityDocumentJson doc = allDocs.get(i);
                        writeFile(pres.getResponse(i).getResponseBody(), DirPath, i);
			System.out.println(String.format("Page %d: %d taxis", pageNo, doc.getValue().size()));
                    }
                    System.out.println("Results processed.");
                    System.out.println();
                    System.out.println();    
                } catch (CompletionException e){
                    System.err.println("An error was encountered with our scraper.");
                    e.printStackTrace();
                }
        }//END WHILE
         
        //ExecutorService HeartbeatExecutor = Executors.newFixedThreadPool(1);
        //HeartbeatExecutor.submit(new HeartBeat());
        
            
    } //END MAIN

    private static void writeFile(String content, String OutputFile, int i) throws IOException{
        Timestamp ts = new Timestamp(System.currentTimeMillis());
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy.MM.dd.HH.mm.SS");
        String TimeStamp = sdf.format(ts);
        String FileName = OutputFile + TimeStamp + ".file" + Integer.toString(i);
        System.out.println(OutputFile);
        try (BufferedWriter bw = new BufferedWriter(new FileWriter(FileName), 16*1024)) {
            bw.write(content);
        }
    }
    
    private static String createDirectory(String OutputFile) throws IOException{
        Timestamp ts = new Timestamp(System.currentTimeMillis());
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy.MM.dd.HH.mm.ss");
        String sts = sdf.format(ts);
        String DirPath = OutputFile + sts + "/";
        System.out.println("DirPath: " + DirPath);
        Path path = Paths.get(DirPath);
        Files.createDirectories(path);
        return DirPath;
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
