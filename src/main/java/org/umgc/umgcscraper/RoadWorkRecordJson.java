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
public class RoadWorkRecordJson {
    @JsonProperty("EventID")
    private  String eventid;
    @JsonProperty("StartDate")
    private String startdate;
    @JsonProperty("EndDate")
    private String enddate;
    @JsonProperty("SvcDept")
    private String svcdept;
    @JsonProperty("RoadName")
    private String roadname;
    @JsonProperty("Other")
    private String other;
    
    public String getEventId() {
        return this.eventid;
    }
    
    public String getStartDate(){
        return this.startdate;        
    }
    
    public String getEndDate(){
        return this.enddate;
    }
    
    public String getSvcDept(){
        return this.svcdept;
    }
    
    public String getRoadName(){
        return this.roadname;
    }
    
    public String getOther(){
        return this.other;
    }
}
