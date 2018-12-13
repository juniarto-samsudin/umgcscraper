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
import java.io.File;
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
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
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
        Object obj = parser.parse(new FileReader("/etc/sb-scraper.conf"));
        JSONObject jsonObject = (JSONObject) obj;
        System.out.println(jsonObject);
        
        
        String OutputFile = (String)jsonObject.get("outputfile");
        System.out.println(OutputFile);
        long loop = (Long)jsonObject.get("loop");
        
        final String accountKey = (String)jsonObject.get("accountkey");
        final ScheduledThreadPoolExecutor scheduler = new ScheduledThreadPoolExecutor(1);
        try (ScraperClient client = ScraperUtil.createScraperClient(8, 50)){
            final long startTimeMillis = ScraperUtil.convertToTimeMillis(2018, 1, 1, 0, 0, 15, ZoneId.of("Asia/Singapore")); //+15 seconds to be safe.
            final long timeStepMillis = 300_000; //every 5 min
            final long maxOvershootMillis = 120_000; //allow to run up to 2 minutes late.
            final long maxRandomDelayMillis = 5_000; //random delay unchanged 5 seconds is good.
            
            final long maxRuntimeMillis = (4*60+30) * 1000L; 
			
            final RealTimeStepper stepper = ScraperUtil.createRealTimeStepper(startTimeMillis, timeStepMillis, maxOvershootMillis, maxRandomDelayMillis);
            final DateTimeFormatter dateTimeFmt = DateTimeFormatter.ISO_LOCAL_DATE_TIME;
            
            final IntFunction<Request> pageCreateFunction = pageNo->{
                String url = String.format("http://datamall2.mytransport.sg/ltaodataservice/TrafficSpeedBandsv2?$skip=%d", pageNo * 500);
                Request req = ScraperUtil.createRequestBuilder().setUrl(url).setHeader("AccountKey", accountKey).build();
		return req;
            };
            
            final Function<Request, CompletableFuture<ScraperResult<SpeedBandDocumentJson>>> pageRequestFunction = req -> {
		return client.requestJson(req, SpeedBandDocumentJson.class);
            };
            
            final int batchSize = client.getMaxConcurrentRequests() * 2;
            
            final Predicate<ScraperResult<SpeedBandDocumentJson>> lastPageTest = (res)->res.getResponseData().getValue().size() < 500;
            final Predicate<ScraperResult<SpeedBandDocumentJson>> emptyPageTest = (res)->res.getResponseData().getValue().isEmpty();
            
            final Predicate<ScraperResult<SpeedBandDocumentJson>> goodResultTest = (res)->{
                    List<SpeedBandRecordJson> list = res.getResponseData().getValue();
                    if (list.size() <= 1) return true; //Good if zero or 1 record (we cannot test).
                    for (int i = 1; i < list.size(); i++) {
			long v1 = list.get(i-1).getLinkId();
			long v2 = list.get(i).getLinkId();
			if (Long.compare(v1, v2) > 0) {
                            //The previous is bigger than the next! Out of order which is correct!
                            return true;
			}
                    }
                    //The page is fully sorted, this is bad!
                    if (list.size() < 50) {
			//This is the last page, give it a chance. 
			//Maybe through some random luck, the unsorted set is actually also sorted by coincidence.
			//Of course if the list is >50 size then it is extremely unlikely that we are mistaken.
			return true;
                    } else {
			return false; //Impossible for so many records to be sorted by coincidence.
                    }
            };
            
            final BiPredicate<Request, Throwable> retryOnErrorTest = (req, t)->false;
            final int maxRetries = 20;
            
            final int retryMinDelayMillis = 1000;
            final int retryMaxDelayMillis = 20000;
            
            final PaginationRequest<SpeedBandDocumentJson> preq = new PaginationRequest<>(
		pageCreateFunction, pageRequestFunction, scheduler, batchSize, 
		lastPageTest, emptyPageTest, goodResultTest, retryOnErrorTest, maxRetries, retryMinDelayMillis, retryMaxDelayMillis
            );
            
            while (true){
                try{
                    System.out.println("Waiting for next step...");
                    stepper.nextStep(); //Sleep until the next step.
                    System.out.println("Step triggered: " + dateTimeFmt.format(LocalDateTime.now()));
					
                    //Determine the deadline to finish. If we are behind this deadline, we must stop.
                    long deadlineMillis = stepper.calcCurrentStepMillis() + maxRuntimeMillis;
                    
                    //Submit the pagination request.
                    CompletableFuture<PaginationResult<SpeedBandDocumentJson>> future = preq.requestPages(deadlineMillis);
                    PaginationResult<SpeedBandDocumentJson> pres = future.join();
                    int size = pres.size(); //Total number of pages returned.
                    List<ScraperResult<SpeedBandDocumentJson>> allResults = pres.getScraperResults();
                    List<SpeedBandDocumentJson> allDocs = pres.getResponseData(); //Get all the response data in a list.
                    int totalLinkCount = 0; //Count the total number of link records.
                    Set<Long> linkSet = new LinkedHashSet<>(); //Track all the observed link ids.
                    
                    String DirPath = createDirectory(OutputFile);
                    String[] temp = DirPath.split(File.separator);
                    String FolderName = temp[temp.length - 1];
                    for (int i = 0; i < size; i++) {
			int pageNo = pres.getPageNumber(i); //Usually pageNo==i, but sometimes you request specific pages only.
			SpeedBandDocumentJson doc = allDocs.get(i);
			System.out.println(String.format("Page %d: %d links", pageNo, doc.getValue().size()));
			totalLinkCount += doc.getValue().size();
			for (SpeedBandRecordJson link : doc.getValue()) {
                            linkSet.add(link.getLinkId()); //Add the link id to the link set, to trace.
			}
                        if ( i == (size-1)){ // LASTPAGE
                            //CHECK LASTPAGE
                            if (checkLastPageOK(linkSet, totalLinkCount, deadlineMillis, pres, preq, client, DirPath)){ // LASTPAGE HAS NO PROBLEM
                                writeFile(pres.getResponse(i).getResponseBody(), DirPath, i);
                                System.out.println("FULL SET COMPLETED!!!!!");
                            }
                        }else{//NOT LASTPAGE, JUST PROCEED...
                            writeFile(pres.getResponse(i).getResponseBody(), DirPath, i);
                        }
                    }
                    String OutputZipFile = OutputFile+FolderName+".zip";
                    Zipper theZipper = new Zipper(DirPath, OutputZipFile);
                    theZipper.zipIt();
                    Metadata theMetadata = new Metadata(OutputZipFile);
                    Messenger theMessenger = new Messenger("speed-band",FolderName,theMetadata.getJsonFile());
                    theMessenger.send();
                    theZipper.delete(new File(DirPath));
                    //If you reach here, allResults & allDocs are correctly fetched, and you can do your own work on them (e.g., publish to Kafka).
                    System.out.println("Results processed.");
                    System.out.println();
                    System.out.println();    
                }catch (CompletionException e) {
                    System.err.println("An error was encountered with our scraper.");
                    e.printStackTrace();
                }
            }
        }
        //ExecutorService HeartbeatExecutor = Executors.newFixedThreadPool(1);
        //HeartbeatExecutor.submit(new HeartBeat());
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
    
    private static boolean checkLastPageOK(Set<Long> linkSet, int totalLinkCount, long deadlineMillis, PaginationResult<SpeedBandDocumentJson> pres,
                                            PaginationRequest<SpeedBandDocumentJson> preq, ScraperClient client, String DirPath ) throws IOException{
        if (linkSet.size() < totalLinkCount) {
			System.out.println("Last page needs to be refreshed.");
			while (true) {
                            //The last page is actually coincidentally sorted. Maybe because the last page size is very small.
                            //We need to keep refreshing the last page until we get the links we want.
                            if (System.currentTimeMillis() < deadlineMillis) {
				throw new CompletionException(new TimeoutException("deadline exceeded on last page retry"));
                            }
                            //Refresh the last page manually.
                            int lastPageNo = pres.getPageNumber(pres.size()-1);
                            Request lastPageReq = preq.createPage(lastPageNo);
                            CompletableFuture<ScraperResult<SpeedBandDocumentJson>> lastPageFuture = client.requestJson(lastPageReq, SpeedBandDocumentJson.class);
                            ScraperResult<SpeedBandDocumentJson> result = lastPageFuture.join();
                            SpeedBandDocumentJson doc = result.getResponseData();
                            for (SpeedBandRecordJson link : doc.getValue()) {
				linkSet.add(link.getLinkId()); //Add the link id to the link set, to trace.
                            }
                            if (linkSet.size() < totalLinkCount) {
				//Still not done. We haven't got the right last page.
				//Sleep for 10 seconds and try again.
				if (System.currentTimeMillis() + 10_000 > deadlineMillis) {
                                    //Nevermind, not safe to sleep.
                                    throw new CompletionException(new TimeoutException("deadline exceeded on last page retry"));
				} else {
                                    ScraperUtil.sleepFor(10_000);
				}
                            } else {
				//We are done.
				//Replace our previous allResults and allDocs lists with copied arrays incorporating our new last page.
				
                                //allResults = new ArrayList<>(allResults);
				//allResults.set(allResults.size()-1, result);
				//allDocs = new ArrayList<>(allDocs);
				//allDocs.set(allDocs.size()-1, doc); 
                                writeFile(result.getResponse().getResponseBody(),DirPath,pres.size()-1);
				System.out.println("Full set of unsorted links acquired.");
				break;
                            }
                            
                            
			}//End While
                        return false;
                    }else{
                        return true;
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
