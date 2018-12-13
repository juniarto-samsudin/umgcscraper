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
public class CarparkAvailabilityRecordJson {
    @JsonProperty("CarParkID")
    private String carparkid;
    @JsonProperty("Area")
    private String area;
    @JsonProperty("Development")
    private String development;
    @JsonProperty("Location")
    private String location;
    @JsonProperty("AvailableLots")
    private int availablelots;
    @JsonProperty("LotType")
    private String lottype;
    @JsonProperty("Agency")
    private String agency;
    
}
