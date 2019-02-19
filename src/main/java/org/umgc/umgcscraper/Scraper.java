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
import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.FileWriter;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.text.SimpleDateFormat;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
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
import org.apache.commons.io.IOUtils;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.HttpClientBuilder;
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
        if (args.length != 1){
            System.out.println("Configuration file path is not provided!");
            System.out.println("Example:  /mnt/scraper.conf");
            System.exit(0);
        }
        
        JSONParser parser = new JSONParser();
        Object obj = parser.parse(new FileReader(args[0]));
        JSONObject jsonObject = (JSONObject) obj;
        
        String URL = (String)jsonObject.get("url");
        String OutputFile = (String)jsonObject.get("outputfile");
        String accountKey = (String)jsonObject.get("accountkey");
        long timestepmillis = (Long)jsonObject.get("timestepmillis");
        long maxovershootmillis = (Long)jsonObject.get("maxovershootmillis");
        long maxrandomdelaymillis = (Long)jsonObject.get("maxrandomdelaymillis");
        long maxruntimemillis = (Long)jsonObject.get("maxruntimemillis");
        String scraperid = (String)jsonObject.get("scraperid");
        int priority = ((Long)jsonObject.get("priority")).intValue();
        String messagetopic = (String)jsonObject.get("messagetopic");
        String bootstrap = (String)jsonObject.get("bootstrap");
        
        System.out.println("-----------------------------------------------");
        System.out.println("URL                 : " + URL); 
        System.out.println("Output Directory    : " + OutputFile);
        System.out.println("TimeStepMillis      : " + timestepmillis);
        System.out.println("MaxOverShootMillis  : " + maxovershootmillis);
        System.out.println("MaxRandomDelayMillis: " + maxrandomdelaymillis);
        System.out.println("MaxRunTimeMillis    : " + maxruntimemillis);
        System.out.println("ScraperId           : " + scraperid);
        System.out.println("Priority            : " + priority);
        System.out.println("MessageTopic        : " + messagetopic);
        System.out.println("Bootstrap Servers   : " + bootstrap);
        System.out.println("------------------------------------------------");
        
        
        final ScheduledThreadPoolExecutor scheduler = new ScheduledThreadPoolExecutor(1);
        ScraperClient client = ScraperUtil.createScraperClient(8, 250);
        
        final long startTimeMillis = ScraperUtil.convertToTimeMillis(2018, 1, 1, 0, 0, 0, ZoneId.of("Asia/Singapore"));
        final long timeStepMillis = timestepmillis;
        final long maxOvershootMillis = maxovershootmillis;
        final long maxRandomDelayMillis = maxrandomdelaymillis;
        final long maxRuntimeMillis = maxruntimemillis; 
        
        final RealTimeStepper stepper = ScraperUtil.createRealTimeStepper(startTimeMillis, timeStepMillis, maxOvershootMillis, maxRandomDelayMillis);
        final DateTimeFormatter dateTimeFmt = DateTimeFormatter.ISO_LOCAL_DATE_TIME;
        
        final IntFunction<Request> pageCreateFunction = pageNo->{
		String url = String.format(URL, pageNo * 500);
                Request req = ScraperUtil.createRequestBuilder().setUrl(url).setHeader("AccountKey", accountKey).build();
		return req;
	};
        
        final Function<Request, CompletableFuture<ScraperResult<TrafficImageDocumentJson>>> pageRequestFunction = req -> {
		return client.requestJson(req, TrafficImageDocumentJson.class);
	};
        
        final int batchSize = client.getMaxConcurrentRequests() * 2;
        
        final Predicate<ScraperResult<TrafficImageDocumentJson>> lastPageTest = (res)->res.getResponseData().getValue().size() < 500;
        
        final Predicate<ScraperResult<TrafficImageDocumentJson>> emptyPageTest = (res)->res.getResponseData().getValue().isEmpty();
        
        final Predicate<ScraperResult<TrafficImageDocumentJson>> goodResultTest = (res)->true;
        
        final BiPredicate<Request, Throwable> retryOnErrorTest = (req, t)->true;
        
        final int maxRetries = 3;
        
        final int retryMinDelayMillis = 1000;
        final int retryMaxDelayMillis = 3000;
        
        final PaginationRequest<TrafficImageDocumentJson> preq = new PaginationRequest<>(
		pageCreateFunction, pageRequestFunction, scheduler, batchSize, 
		lastPageTest, emptyPageTest, goodResultTest, retryOnErrorTest, maxRetries, retryMinDelayMillis, retryMaxDelayMillis
	);
        
        
        HashSet<String> ExistingImageSet = new HashSet<>();
        int loop = 0;
        long timeMillis=0;
        while (true){
            try{
                    System.out.println("Waiting for next step...");
                    timeMillis = stepper.nextStep(); //Sleep until the next step.
                    System.out.println("Step triggered: " + dateTimeFmt.format(LocalDateTime.now()));
            
                    long deadlineMillis = stepper.calcCurrentStepMillis() + maxRuntimeMillis;
                    
                    CompletableFuture<PaginationResult<TrafficImageDocumentJson>> future = preq.requestPages(deadlineMillis);
                    PaginationResult<TrafficImageDocumentJson> pres = future.join();
                    int size = pres.size(); //Total number of pages returned.
                    List<TrafficImageDocumentJson> allDocs = pres.getResponseData(); //Get all the response data in a list.
                    
                    String[] PathAll = createDirectory(OutputFile, timeMillis);
                    //Image Path
                    String DirPath = PathAll[0];
                    String DirPathZip = PathAll[1];
                    String ImagePath = PathAll[2];
                    String[] temp = DirPath.split(File.separator);
                    String FolderName = temp[temp.length - 1];
                    System.out.println(FolderName);
                    
                    for (int i = 0; i < size; i++) {
			int pageNo = pres.getPageNumber(i); //Usually pageNo==i, but sometimes you request specific pages only.
			TrafficImageDocumentJson doc = allDocs.get(i);
                        
                        List<Request> AwsRequestList = new ArrayList<>();
                        List<String> ImageNameList = new ArrayList<>();
                        int newDownload=0;
                        int existing= 0;
                        for (int j = 0; j < doc.getValue().size(); j++){
                            //System.out.println("ImageLink: " + doc.getValue().get(j).getImageLink());
                            //awsImageDownload(OutputFile, doc.getValue().get(j).getImageLink());
                            String AwsUrl = doc.getValue().get(j).getImageLink();
                            String[] AwsUrlSplit = AwsUrl.split("/");
                            //System.out.println("TrueFalse: " + ExistingImageSet.contains(AwsUrlSplit[AwsUrlSplit.length - 1]));
                            if (!ExistingImageSet.contains(AwsUrlSplit[AwsUrlSplit.length - 1])){
                                //System.out.println("Add : " + AwsUrlSplit[AwsUrlSplit.length - 1]);
                                newDownload = newDownload + 1;
                                ExistingImageSet.add(AwsUrlSplit[AwsUrlSplit.length - 1]);
                                String ImageFileName = OutputFile + "images/" +  AwsUrlSplit[AwsUrlSplit.length - 1];
                                //System.out.println("ImageFileName: " + ImageFileName);
                                ImageNameList.add(ImageFileName);
                                Request req = ScraperUtil.createRequestBuilder().setUrl(AwsUrl).build();
                                AwsRequestList.add(req);
                            }else{
                                existing = existing + 1;
                                //System.out.println("Link already exist: !!!!!!!" + AwsUrl);
                            }      
                        }
                        
                        System.out.println("HashSet Size: " + ExistingImageSet.size());
                        System.out.println("New Download: " + newDownload);
                        System.out.println("Existing    : " + existing);
                        
                        final List<CompletableFuture<ScraperResult<byte[]>>> imageFutures = new ArrayList<>();
                        for (int j = 0; j < AwsRequestList.size(); j++){
                            Request req = AwsRequestList.get(j);
                            CompletableFuture<ScraperResult<byte[]>> imageFuture = client.requestBytes(req);
                            imageFutures.add(imageFuture);
						
                        }
                        
                        for (int j = 0; j < AwsRequestList.size(); j++){
                            CompletableFuture<ScraperResult<byte[]>> imageFuture = imageFutures.get(j);
                            ScraperResult<byte[]> imageResult = imageFuture.join();
                            InputStream data = new BufferedInputStream(imageResult.getResponse().getResponseBodyAsStream());
                            OutputStream out = new BufferedOutputStream(new FileOutputStream(new File(ImageNameList.get(j))));
                            IOUtils.copy(data, out);
                            out.flush();
                            out.close();
                        
                        }
                        
                        
                        writeFile(pres.getResponse(i).getResponseBody(), DirPath, i, timeMillis);
                        System.out.println(String.format("Page %d: %d traffic-images", pageNo, doc.getValue().size()));
                    }
                    //String OutputZipFile = OutputFile+FolderName+".zip";
                    String OutputZipFile = DirPathZip + FolderName + ".zip";
                    System.out.println("OutputZipFile : " + OutputZipFile);
                    Zipper theZipper = new Zipper(DirPath,OutputZipFile);
                    theZipper.zipIt();
                    Metadata theMetadata = new Metadata(OutputZipFile, scraperid, priority, ImagePath);
                    
                    Messenger theMessenger = new Messenger(messagetopic,FolderName,theMetadata.getJsonFile(), bootstrap);
                    theMessenger.send();
                    theZipper.delete(new File(DirPath));
                    System.out.println("Results processed.");
                    System.out.println();
                    System.out.println();   
                    loop = loop + 1;
                    if (loop == 1440 ){
                        loop = 0;
                        ExistingImageSet.clear();
                        System.out.println("HashSet Clear");
                    }
                } catch (CompletionException e){
                    SimpleDateFormat sdf = new SimpleDateFormat("yyyy.MM.dd.HH.mm.SS");
                    String TimeStamp = sdf.format(timeMillis);
                    System.err.println(TimeStamp + " An error was encountered with our scraper.");
                    e.printStackTrace();
                }
        }//END WHILE
         
        //ExecutorService HeartbeatExecutor = Executors.newFixedThreadPool(1);
        //HeartbeatExecutor.submit(new HeartBeat());
        
            
    } //END MAIN

    private static void writeFile(String content, String OutputFile, int i, long timeMillis) throws IOException{
        //Timestamp ts = new Timestamp(System.currentTimeMillis());
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy.MM.dd.HH.mm.SS");
        String TimeStamp = sdf.format(timeMillis);
        String FileName = OutputFile + TimeStamp + ".file" + Integer.toString(i);
        System.out.println(OutputFile);
        try (BufferedWriter bw = new BufferedWriter(new FileWriter(FileName), 16*1024)) {
            bw.write(content);
        }
    }
    
    private static String[] createDirectory(String OutputFile, long timeMillis) throws IOException{
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy.MM.dd.HH.mm.ss");
        String sts = sdf.format(timeMillis);
        
        TimeProcessor tp = new TimeProcessor(sts);
        String DirPath = OutputFile + tp.getYear() + "/" +
                                      tp.getMonth()+ "/" +
                                      tp.getDate() + "/" +
                                      sts + "/";
        String DirPathZip = OutputFile + tp.getYear() + "/" +
                                      tp.getMonth()+ "/" +
                                      tp.getDate() + "/";
       
        System.out.println("DirPath: " + DirPath);
        Path path = Paths.get(DirPath);
        Files.createDirectories(path);
        //Image Directory
        String ImagePath = OutputFile + "images" + "/";
        Path imagepath = Paths.get(ImagePath);
        if(!Files.exists(imagepath)){
            Files.createDirectories(imagepath);
        }
        String[] PathAll = {DirPath, DirPathZip, ImagePath};
        return PathAll;
    }
    
    private static void awsImageDownload(String OutputFile, String AwsUrl) throws IOException{
        System.out.println(AwsUrl);
        String[] temp = AwsUrl.split("/");
        System.out.println(temp[temp.length - 1]);
        String ImageFileName = OutputFile + "images/" +  temp[temp.length - 1];
        
        
        
        HttpClient AwsClient = HttpClientBuilder.create().build();
        HttpGet AwsRequest = new HttpGet(AwsUrl);
        int bufferSize = 16 *1024;
        HttpResponse response = AwsClient.execute(AwsRequest);
        System.out.println("Response Code: " + response.getStatusLine().getStatusCode());     
        InputStream data = new BufferedInputStream(response.getEntity().getContent());
        OutputStream out = new BufferedOutputStream(new FileOutputStream(new File(ImageFileName)));
                    
        IOUtils.copy(data, out);
        out.flush();
        out.close();
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

class TimeProcessor{
    private final String Year;
    private final String Month;
    private final String Date;
    //example: yyyy.MM.dd.HH.mm.ss
    TimeProcessor(String stringtime){
        String[] temp = stringtime.split("\\.");
        this.Year = temp[0];
        this.Month = temp[1];
        this.Date = temp[2];
    }
    
    public String getYear(){
        return Year;
    }
    
    public String getMonth(){
        return Month;
    }
    
    public String getDate(){
        return Date;
    }
}
