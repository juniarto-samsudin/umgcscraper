/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.umgc.umgcscraper;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

/**
 *
 * @author samsudinj
 */
public class Zipper {
    private final List<String> fileList;
    private final String SourceDirectory;
    private final String OutputZipFile;
    
    Zipper(String SourceDirectory, String OutputZipFile){
        fileList = new ArrayList<>();
        this.SourceDirectory = SourceDirectory;
        this.OutputZipFile = OutputZipFile;
        generateFileList(new File(SourceDirectory));
    }
    
    public void zipIt(){
        byte[] buffer = new byte[1024];
    	
        try{
    		
            FileOutputStream fos = new FileOutputStream(OutputZipFile);
            ZipOutputStream zos = new ZipOutputStream(fos);
    		
            System.out.println("Output to Zip : " + OutputZipFile);
    		
            for(String file : this.fileList){
    			
    		//System.out.println("File Added : " + file);
    		ZipEntry ze= new ZipEntry(file);
        	zos.putNextEntry(ze);
               
        	FileInputStream in = 
                       new FileInputStream(SourceDirectory + File.separator + file);
       	   
        	int len;
        	while ((len = in.read(buffer)) > 0) {
        		zos.write(buffer, 0, len);
        	}
               
        	in.close();
            }
    		
            zos.closeEntry();
            //remember close it
            zos.close();
          
            System.out.println("Done");
        }catch(IOException ex){
            ex.printStackTrace();   
        }
    }
    
    private void generateFileList(File node){
        	//add file only
	if(node.isFile()){
                //System.out.println("ITS A FILE!");
                //System.out.println(node.getAbsoluteFile().toString());
		fileList.add(generateZipEntry(node.getAbsoluteFile().toString()));
	}
		
	if(node.isDirectory()){
                System.out.println("ITS A DIR!");
		String[] subNote = node.list();
		for(String filename : subNote){
			generateFileList(new File(node, filename));
		}
	}
    }
    
    private String generateZipEntry(String file){
        //System.out.println("SourceDirectory: " + SourceDirectory);
        //System.out.println("SourceDirectory length: " + SourceDirectory.length());
        //System.out.println("file lenght: " + file.length());
    	return file.substring(SourceDirectory.length(), file.length());
    
    }
    
    public void delete(File file){
        if (file.isDirectory()){
            if (file.list().length==0){
                file.delete();
                System.out.println("Empty directory is deleted");
            } else {
                String files[] =  file.list();
                for (String temp: files){
                File fileDelete = new File(file, temp);
                delete(fileDelete);
            }
            if (file.list().length == 0){
                file.delete();
            }    
        }
    }else{
            file.delete();
            //System.out.println("File is deleted !");
        }
}
}
