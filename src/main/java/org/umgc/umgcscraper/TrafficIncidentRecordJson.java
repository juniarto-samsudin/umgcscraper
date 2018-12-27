package org.umgc.umgcscraper;
import com.fasterxml.jackson.annotation.JsonProperty;
/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

/**
 *
 * @author samsudinj
 */
public class TrafficIncidentRecordJson {
    @JsonProperty("Type")
    private String type;
    @JsonProperty("Latitude")
    private double latitude;
    @JsonProperty("Longitude")
    private double longitude;
    @JsonProperty("Message")
    private String message;
    
}
