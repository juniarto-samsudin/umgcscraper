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
import java.util.concurrent.Callable;
import java.util.logging.Level;
import java.util.logging.Logger;
import static org.apache.http.HttpHeaders.USER_AGENT;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.util.EntityUtils;
import org.json.simple.parser.ParseException;
import org.umgc.umgcscraper.parser.TaxiAvailabilityParser;

/**
 *
 * @author samsudinj
 */
public class TaxiAvailabilityCallable implements Callable<Integer> {
    private final String url;
    private final String OutputFile;
    private int skip;
    private int contSignal;
    
    public TaxiAvailabilityCallable (String url, int skip, String OutputFile, int contSignal){
        this.url = url;
        this.OutputFile = OutputFile;
        this.skip = skip;
        this.contSignal = contSignal;
    }
    
    @Override
    public Integer call() throws Exception {
            HttpClient client = HttpClientBuilder.create().build();
            HttpGet request = new HttpGet(url+Integer.toString(skip));    
            request.addHeader("User-Agent", USER_AGENT);
            request.addHeader("AccountKey","WT9HkF2lS6S7qfL1u6IOCA==");
            request.addHeader("accept","application/json");
            int n = 0;
            int bufferSize = 16 *1024;
            try {
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
                if (theParser.getNumberOfPosition() < 500){
                    contSignal = 0;
                }
                if (theParser.getNumberOfPosition() != 0){
                    Timestamp ts = new Timestamp(System.currentTimeMillis());
                    SimpleDateFormat sdf = new SimpleDateFormat("yyyy.MM.dd.HH.mm.SS");
                    
                    String TimeStamp = sdf.format(ts);
                    String FileName = OutputFile + TimeStamp + ".skip" + Integer.toString(skip);
                    System.out.println(OutputFile);
                    BufferedWriter bw = new BufferedWriter(new FileWriter(FileName), 16*1024);
                    bw.write(result.toString());
                    bw.close();
                    n++;
                }
            } catch (IOException | ParseException ex) { 
                Logger.getLogger(TaxiAvailability.class.getName()).log(Level.SEVERE, null, ex);
            }
        
        return contSignal;
    }
    
}
