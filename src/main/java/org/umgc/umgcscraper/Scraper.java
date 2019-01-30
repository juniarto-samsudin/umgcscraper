/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.umgc.umgcscraper;

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
        
        JSONParser parser = new JSONParser();
        Object obj = parser.parse(new FileReader("/etc/pvodtrain-scraper.conf"));
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
         
        ScraperClient client = ScraperUtil.createScraperClient(8, 250);
        
        final long startTimeMillis = ScraperUtil.convertToTimeMillis(2018, 1, 1, 0, 0, 0, ZoneId.of("Asia/Singapore"));
        //final long timeStepMillis = 60_000;
        final long timeStepMillis = timestepmillis;  //DAILY
        final long maxOvershootMillis = maxovershootmillis;
        final long maxRandomDelayMillis = maxrandomdelaymillis;
        
        final long maxRuntimeMillis = maxruntimemillis; 
        
        final RealTimeStepper stepper = ScraperUtil.createRealTimeStepper(startTimeMillis, timeStepMillis, maxOvershootMillis, maxRandomDelayMillis);
        final DateTimeFormatter dateTimeFmt = DateTimeFormatter.ISO_LOCAL_DATE_TIME;
        
        List<Map<String,String>> StateList = new ArrayList<>();
        while (true){
            try{
                    System.out.println("Waiting for next step...");
                    long timeMillis = stepper.nextStep(); //Sleep until the next step.
                    System.out.println("Step triggered: " + dateTimeFmt.format(LocalDateTime.now()));
            
                    Map<String, String> CurState = new HashMap<>();
                    
                    String url = URL;
                    Request req = ScraperUtil.createRequestBuilder().setUrl(url).setHeader("AccountKey", accountKey).build();
                    CompletableFuture<ScraperResult<PasVolOdBusDocumentJson>> future = client.requestJson(req, PasVolOdBusDocumentJson.class);
                    ScraperResult<PasVolOdBusDocumentJson> result = future.join();
                    PasVolOdBusDocumentJson content = result.getResponseData();
                    System.out.println("RESULT IS: " + content.getValue().get(0).getLink());
                    
                    
                    String ZipFile = createZipFile(OutputFile, timeMillis);
                    
                    String AwsUrl = content.getValue().get(0).getLink();
                    HttpClient AwsClient = HttpClientBuilder.create().build();
                    HttpGet AwsRequest = new HttpGet(AwsUrl);
                    int bufferSize = 16 *1024;
                    HttpResponse response = AwsClient.execute(AwsRequest);
                    System.out.println("Response Code: " + response.getStatusLine().getStatusCode());
                    
                    InputStream data = new BufferedInputStream(response.getEntity().getContent());
                    OutputStream out = new BufferedOutputStream(new FileOutputStream(new File(ZipFile)));
                    
                    IOUtils.copy(data, out);
                    out.flush();
                    out.close();
                    
                    Metadata theMetadata = new Metadata(ZipFile, scraperid, priority);
                    CurState.put("file", theMetadata.getSha256Hex());
                    CurState.put("date", theMetadata.getCurDate());
                    CurState.put("month", theMetadata.getCurMonth());
                    
                    StateList.add(CurState);
                    //AT THE BEGINNING
                    //ALWAYS SEND
                    if (StateList.size() == 1){
                        System.out.println("FIRST ENTRY! JUST GO AHEAD");
                        Messenger theMessenger = new Messenger(messagetopic,OutputFile,theMetadata.getJsonFile(),bootstrap);
                        theMessenger.send();
                        System.out.println(theMetadata.getCurDate());
                    } else if (StateList.size() > 1){
                        System.out.println("OTHER THAN FIRST ENTRY!");
                        //CHECK MD5CHECKSUM  
                        Map<String, String> map0 = StateList.get(0);
                        Map<String, String> map1 = StateList.get(1);
                        int date0 = Integer.parseInt(map0.get("date"));
                        int date1 = Integer.parseInt(map1.get("date"));
                        if (map0.get("file").equals(map1.get("file"))){//IF THE MD5SUM IS THE SAME
                            System.out.println("MD5SUM IS THE SAME!");
                            if (map0.get("month").equals(map1.get("month"))){
                                System.out.println("Same Month!");
                                if (date1 > 15 && date0 <=15){
                                    System.out.println("date1 > 15 and date0 <=15");
                                    Messenger theMessenger = new Messenger(messagetopic,OutputFile,theMetadata.getJsonFile(),bootstrap);
                                    theMessenger.send();
                                    StateList.remove(0);
                                }else{
                                    System.out.println("delete zip file");
                                    Zipper theZipper = new Zipper(OutputFile,ZipFile);
                                    theZipper.delete(new File(ZipFile)); //DELETE THE ZIPFILE
                                    StateList.remove(1);
                                }
                            }else{
                                System.out.println("Different Month!");
                                if (date1 > 15 && date0 > 15){
                                    System.out.println("date1 > 15 and date0 >15");
                                    Messenger theMessenger = new Messenger(messagetopic,OutputFile,theMetadata.getJsonFile(),bootstrap);
                                    theMessenger.send();
                                    StateList.remove(0);
                                }else{
                                    System.out.println("delete zip file");
                                    Zipper theZipper = new Zipper(OutputFile,ZipFile);
                                    theZipper.delete(new File(ZipFile)); //DELETE THE ZIPFILE
                                    StateList.remove(1);
                                }
                            }
                            
                            
                        }else{//MD5SUM IS DIFFERENT, SURE SEND
                            Messenger theMessenger = new Messenger(messagetopic,OutputFile,theMetadata.getJsonFile(),bootstrap);
                            theMessenger.send();
                            System.out.println(theMetadata.getCurDate());
                            StateList.remove(0);
                        }
                    }
                    
                    /*
                    InputStream data = response.getEntity().getContent();
                    ByteArrayOutputStream baos = new ByteArrayOutputStream(1024 *1024);
                    try{
                    IOUtils.copy(data, baos);
                    }finally{
                        data.close();
                    }
                    System.out.println("WRITE COMPLETED!");
                    */
                    
                    /*
                    InputStream data = response.getEntity().getContent();
                    OutputStream output = new FileOutputStream("response.zip");
                    byte[] buffer = new byte[1024*6];
                    while(data.read(buffer) != -1){
                        output.write(buffer);
                    }
                    output.flush();
                    output.close();
                    */
                    System.out.println("Write Completed!!!");
                    
                    
                    /*
                    long deadlineMillis = stepper.calcCurrentStepMillis() + maxRuntimeMillis;
                    
                    
                    int size = pres.size(); //Total number of pages returned.
                    List<TaxiAvailabilityDocumentJson> allDocs = pres.getResponseData(); //Get all the response data in a list.
                    
                    String DirPath = createDirectory(OutputFile);
                    System.out.println("DIRPATH : " + DirPath);
                    String[] temp = DirPath.split(File.separator);
                    String FolderName = temp[temp.length - 1];
                    System.out.println(FolderName);
                    for (int i = 0; i < size; i++) {
			int pageNo = pres.getPageNumber(i); //Usually pageNo==i, but sometimes you request specific pages only.
			TaxiAvailabilityDocumentJson doc = allDocs.get(i);
                        writeFile(pres.getResponse(i).getResponseBody(), DirPath, i);
			System.out.println(String.format("Page %d: %d taxis", pageNo, doc.getValue().size()));
                    }
                    String OutputZipFile = OutputFile+FolderName+".zip";
                    System.out.println("OutputZipFile : " + OutputZipFile);
                    Zipper theZipper = new Zipper(DirPath,OutputZipFile);
                    theZipper.zipIt();
                    Metadata theMetadata = new Metadata(OutputZipFile);
                    //System.out.println("ZIPTIMESTAMP: " + theMetadata.getTimeStamp());
                    //System.out.println("ZIPMD5: " + theMetadata.getMd5Hash());
                    //System.out.println("FILEPATH: " + theMetadata.getFilePath());
                    //System.out.println("JSON: " + theMetadata.getJsonFile());
                    
                    Messenger theMessenger = new Messenger("taxi-availability",FolderName,theMetadata.getJsonFile());
                    theMessenger.send();
                    theZipper.delete(new File(DirPath));
                    System.out.println("Results processed.");
                    System.out.println();
                    System.out.println();
                    */
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
    
    private static String createZipFile(String OutputFile, long timeMillis) throws IOException{
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy.MM.dd.HH.mm.ss");
        String sts = sdf.format(timeMillis);
        TimeProcessor tp = new TimeProcessor(sts);
        
        String ZipFile = OutputFile + tp.getYear() + "/" +
                                      tp.getMonth()+ "/" +
                                      tp.getDate() + "/" +
                                      sts + ".zip";
        String DirPathZip = OutputFile + tp.getYear() + "/" +
                                      tp.getMonth()+ "/" +
                                      tp.getDate() + "/";
        Path path = Paths.get(DirPathZip);
        Files.createDirectories(path);
        //String ZipFile = OutputFile + sts + ".zip";
        return ZipFile;
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
