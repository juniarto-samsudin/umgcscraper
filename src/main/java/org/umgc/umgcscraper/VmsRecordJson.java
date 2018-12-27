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
public class VmsRecordJson {
    @JsonProperty("EquipmentID")
    private String equipmentid;
    @JsonProperty("Latitude")
    private double latitude;
    @JsonProperty("Longitude")
    private double longitude;
    @JsonProperty("Message")
    private String message;
}
