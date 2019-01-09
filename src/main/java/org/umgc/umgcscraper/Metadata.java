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
    
    Metadata(String OutputZipFile){
        this.OutputZipFile = OutputZipFile;
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
    
    //Scraper ID
    public int getScraperId(){
        return 1;
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
    public String getMd5Hash() throws FileNotFoundException, IOException{
        FileInputStream theinputstream = new FileInputStream(OutputZipFile);
        String md5Hex = DigestUtils.md5Hex(theinputstream);
        return md5Hex;
    }
    
    //JSON
    public String getJsonFile() throws IOException{
        JSONObject obj = new JSONObject();
        obj.put("scraper_id",getScraperId());
        obj.put("request_timestamp",getTimeStamp());
        obj.put("file_path",getFilePath());
        obj.put("file_hash",getMd5Hash());
        obj.put("file_length",getFileSize());
        return obj.toJSONString();
    }
}
