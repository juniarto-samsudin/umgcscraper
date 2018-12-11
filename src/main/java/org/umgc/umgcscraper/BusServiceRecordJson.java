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
public class BusServiceRecordJson {
	@JsonProperty("ServiceNo")
	private String serviceno;
	@JsonProperty("Operator")
	private String operator;
        @JsonProperty("Direction")
        private int direction;
        @JsonProperty("Category")
        private String category;
        @JsonProperty("OriginCode")
        private String origincode;
        @JsonProperty("DestinationCode")
        private String destinationcode;
        @JsonProperty("AM_Peak_Freq")
        private String am_peak_freq;
        @JsonProperty("AM_Offpeak_Freq")
        private String am_offpeak_freq;
        @JsonProperty("PM_Peak_Freq")
        private String pm_peak_freq;
        @JsonProperty("PM_Offpeak_Freq")
        private String pm_offpeak_freq;
        @JsonProperty("LoopDesc")
        private String loopdesc;
                
	
        /*
	public double getLongitude() {
		return longitude;
	}

	public void setLongitude(double longitude) {
		this.longitude = longitude;
	}

	public double getLatitude() {
		return latitude;
	}

	public void setLatitude(double latitude) {
		this.latitude = latitude;
	}

	@Override
	public String toString() {
		return "(" + longitude + ", " + latitude + ")";
	}
	*/
}
