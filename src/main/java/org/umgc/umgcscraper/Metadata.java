/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.umgc.umgcscraper;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import org.apache.commons.codec.digest.DigestUtils;
import org.json.simple.JSONObject;

/**
 *
 * @author samsudinj
 */
public class Metadata {
    
    private final String OutputZipFile;
    private final String ScraperId;
    private final int Priority;
    
    Metadata(String OutputZipFile, String ScraperId, int Priority){
        this.OutputZipFile = OutputZipFile;
        this.ScraperId = ScraperId;
        this.Priority = Priority;
    }
    
    
    
    //File Size
    public double getFileSize(){
        File file = new File(OutputZipFile);
        double bytes = file.length();
        return bytes;
    }
    
    //File Path
    public String getFilePath(){
        return OutputZipFile;
    }
    
    //File Name
    public String getFileName() {
	int ix = OutputZipFile.lastIndexOf('/');
	if (ix == -1) return OutputZipFile;
	return OutputZipFile.substring(ix+1);
    }

    //File Dir
    public String getFileDir() {
	int ix = OutputZipFile.lastIndexOf('/');
	if (ix == -1) return "/";
	return OutputZipFile.substring(0, ix+1);
    }
    
    //Scraper ID
    public String getScraperId(){
        return ScraperId;
    }
    
    public int getPriority(){
        return Priority;
    }
    
    //TimeStamp
    public String getTimeStamp(){
        String[] temp = OutputZipFile.split(File.separator);
        String ZipFileName = temp[temp.length - 1];
        String TimeStamp = ZipFileName.substring(0, ZipFileName.length() - 4);
        System.out.println("TimeStamp: " + TimeStamp);
        return TimeStamp;
    }
    
    //Hash
    public String getSha256Hex() throws FileNotFoundException, IOException{
        FileInputStream theinputstream = new FileInputStream(OutputZipFile);
        String md5Hex = DigestUtils.sha256Hex(theinputstream);
        return md5Hex;
    }
    
    //JSON
    public String getJsonFile() throws IOException{
        JSONObject obj = new JSONObject();
        obj.put("scraper_id",getScraperId());
        obj.put("request_timestamp",getTimeStamp());
        obj.put("file_path",getFilePath());
        obj.put("file_dir",getFileDir());
        obj.put("file_name",getFileName());
        obj.put("file_hash",getSha256Hex());
        obj.put("file_length",getFileSize());
        return obj.toJSONString();
    }
}
