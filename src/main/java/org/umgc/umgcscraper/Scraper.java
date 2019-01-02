/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.umgc.umgcscraper;

import astar.ihpc.umgc.scraper.util.PaginationRequest;
import astar.ihpc.umgc.scraper.util.PaginationResult;
import astar.ihpc.umgc.scraper.util.RealTimeStepper;
import astar.ihpc.umgc.scraper.util.ScraperClient;
import astar.ihpc.umgc.scraper.util.ScraperResult;
import astar.ihpc.umgc.scraper.util.ScraperUtil;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ScheduledThreadPoolExecutor;
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
        Object obj = parser.parse(new FileReader("/etc/bus-stop-scraper.conf"));
        JSONObject jsonObject = (JSONObject) obj;
        System.out.println(jsonObject);
        
        String OutputFile = (String)jsonObject.get("outputfile");
        System.out.println(OutputFile);
        
        String accountKey = (String)jsonObject.get("accountkey");
        
        final ScheduledThreadPoolExecutor scheduler = new ScheduledThreadPoolExecutor(1);
        ScraperClient client = ScraperUtil.createScraperClient(8, 250);
        
        final long startTimeMillis = ScraperUtil.convertToTimeMillis(2018, 1, 1, 0, 0, 0, ZoneId.of("Asia/Singapore"));
        final long timeStepMillis = 10800_000;//EVERY 3 HOURS
        //final long timeStepMillis = 60_000;
        final long maxOvershootMillis = 20_000;
        final long maxRandomDelayMillis = 5_000;
        
        final long maxRuntimeMillis = 35_000; 
        
        final RealTimeStepper stepper = ScraperUtil.createRealTimeStepper(startTimeMillis, timeStepMillis, maxOvershootMillis, maxRandomDelayMillis);
        final DateTimeFormatter dateTimeFmt = DateTimeFormatter.ISO_LOCAL_DATE_TIME;
        
        final IntFunction<Request> pageCreateFunction = pageNo->{
		String url = String.format("http://datamall2.mytransport.sg/ltaodataservice/BusStops?$skip=%d", pageNo * 500);
		Request req = ScraperUtil.createRequestBuilder().setUrl(url).setHeader("AccountKey", accountKey).build();
		return req;
	};
        
        final Function<Request, CompletableFuture<ScraperResult<BlackBoxLtaDataMallDocumentJson>>> pageRequestFunction = req -> {
		return client.requestJson(req, BlackBoxLtaDataMallDocumentJson.class);
	};
        
        final int batchSize = client.getMaxConcurrentRequests() * 2;
        
        final Predicate<ScraperResult<BlackBoxLtaDataMallDocumentJson>> lastPageTest = (res)->res.getResponseData().getValue().size() < 500;
        
        final Predicate<ScraperResult<BlackBoxLtaDataMallDocumentJson>> emptyPageTest = (res)->res.getResponseData().getValue().isEmpty();
        
        final Predicate<ScraperResult<BlackBoxLtaDataMallDocumentJson>> goodResultTest = (res)->true;
        
        final BiPredicate<Request, Throwable> retryOnErrorTest = (req, t)->true;
        
        final int maxRetries = 3;
        
        final int retryMinDelayMillis = 1000;
        final int retryMaxDelayMillis = 3000;
        
        final PaginationRequest<BlackBoxLtaDataMallDocumentJson> preq = new PaginationRequest<>(
		pageCreateFunction, pageRequestFunction, scheduler, batchSize, 
		lastPageTest, emptyPageTest, goodResultTest, retryOnErrorTest, maxRetries, retryMinDelayMillis, retryMaxDelayMillis
	);
        
        List<Map<String,String>> StateList = new ArrayList<>();
        while (true){
            try{
                    System.out.println("Waiting for next step...");
                    stepper.nextStep(); //Sleep until the next step.
                    System.out.println("Step triggered: " + dateTimeFmt.format(LocalDateTime.now()));
            
                    Map<String, String> CurState = new HashMap<>();
                    
                    long deadlineMillis = stepper.calcCurrentStepMillis() + maxRuntimeMillis;
                    
                    CompletableFuture<PaginationResult<BlackBoxLtaDataMallDocumentJson>> future = preq.requestPages(deadlineMillis);
                    PaginationResult<BlackBoxLtaDataMallDocumentJson> pres = future.join();
                    int size = pres.size(); //Total number of pages returned.
                    List<BlackBoxLtaDataMallDocumentJson> allDocs = pres.getResponseData(); //Get all the response data in a list.
                    
                    String DirPath = createDirectory(OutputFile);
                    System.out.println("DIRPATH : " + DirPath);
                    String[] temp = DirPath.split(File.separator);
                    String FolderName = temp[temp.length - 1];
                    System.out.println(FolderName);
                    for (int i = 0; i < size; i++) {
			int pageNo = pres.getPageNumber(i); //Usually pageNo==i, but sometimes you request specific pages only.
			//TaxiAvailabilityDocumentJson doc = allDocs.get(i);
                        writeFile(pres.getResponse(i).getResponseBody(), DirPath, i, CurState);
			//System.out.println(String.format("Page %d: %d taxis", pageNo, doc.getValue().size()));
                    }
                    StateList.add(CurState);
                    
                    String OutputZipFile = OutputFile+FolderName+".zip";
                    System.out.println("OutputZipFile : " + OutputZipFile);
                    if (StateList.size() == 1){  // At the beginning
                        //ALWAYS ZIP, DELETE AND SEND
                        System.out.println("AT THE BEGINNING, ALWAYS WRITE");
                        ZipAndDelete(DirPath, OutputZipFile, FolderName);
                    }else{
                        Map<String,String> map0 = StateList.get(0);
                        Map<String,String> map1 = StateList.get(1);
                        System.out.println("Map 0 size: " + map0.size());
                        System.out.println("Map 1 size: " + map1.size());
                        if (map0.size() != map1.size()){
                            System.out.println("Map Size different!!!!Write as Usual!");
                            //WRITE AS USUAL
                            ZipAndDelete(DirPath, OutputZipFile, FolderName);
                            StateList.remove(0);//REMOVE THE OLD ONE
                        }else{//IF THE SIZE OF THE MAP IS THE SAME: 
                            if (map0.equals(map1)){
                                System.out.println("Both Map are Equal, No Need To Write!!!");
                                //NO NEED TO WRITE BUT STILL NEED TO DELETE
                                Zipper theZipper = new Zipper(DirPath, OutputZipFile);
                                theZipper.delete(new File(DirPath));
                                StateList.remove(1);//REMOVE THE NEW ONE, NEW ONE INVALID AND REPLACED.
                            }else{
                                System.out.println("Both Maps are not equal!");
                                ZipAndDelete(DirPath, OutputZipFile, FolderName);
                                StateList.remove(0);//REMOVE THE OLD ONE, OLD ONE INVALID AND REPLACED.
                            }
                        }
                        
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
    
    
    private static void ZipAndDelete(String DirPath, String OutputZipFile, String FolderName) throws IOException{
        Zipper theZipper = new Zipper(DirPath,OutputZipFile);
        theZipper.zipIt();
        Metadata theMetadata = new Metadata(OutputZipFile);
        //System.out.println("ZIPTIMESTAMP: " + theMetadata.getTimeStamp());
        //System.out.println("ZIPMD5: " + theMetadata.getMd5Hash());
        //System.out.println("FILEPATH: " + theMetadata.getFilePath());
        //System.out.println("JSON: " + theMetadata.getJsonFile());
                    
        Messenger theMessenger = new Messenger("bus-stop",FolderName,theMetadata.getJsonFile());
        theMessenger.send();
        theZipper.delete(new File(DirPath));
    }

    private static void writeFile(String content, String OutputFile, int i, Map<String,String> CurState) throws IOException{
        Timestamp ts = new Timestamp(System.currentTimeMillis());
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy.MM.dd.HH.mm.SS");
        String TimeStamp = sdf.format(ts);
        String FileName = OutputFile + TimeStamp + ".file" + Integer.toString(i);
        System.out.println(OutputFile);
        try (BufferedWriter bw = new BufferedWriter(new FileWriter(FileName), 16*1024)) {
            bw.write(content);
        }
        Metadata filemetadata = new Metadata(FileName);
        String key = "file" + Integer.toString(i);
        CurState.put(key, filemetadata.getMd5Hash() );
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
