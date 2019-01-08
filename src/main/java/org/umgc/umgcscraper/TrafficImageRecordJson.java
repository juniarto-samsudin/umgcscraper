/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.umgc.umgcscraper;
import com.fasterxml.jackson.annotation.JsonProperty;
/**
 *
 * @author samsudinj
 */
public class TrafficImageRecordJson {
     @JsonProperty("CameraID")
    private String cameraid;
    @JsonProperty("Latitude")
    private double latitude;
    @JsonProperty("Longitude")
    private double longitude;
    @JsonProperty("ImageLink")
    private String imagelink;
    
    public String getImageLink(){
        return imagelink;
    }
    
    public double getLongitude(){
        return longitude;
    }
    
    public double getLatitude(){
        return latitude;
    }
    
    public String getCameraId(){
        return cameraid;
    }
    
    public void setImageLink(String link){
        this.imagelink = link;
    }
}
