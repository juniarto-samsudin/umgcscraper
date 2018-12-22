package astar.ihpc.umgc.scraper.sources.ltadatamall;

import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * The record for Taxi Availability. Just lng & lat.
 * @author othmannb
 *
 */
public class TaxiAvailabilityRecordJson {
	@JsonProperty("Longitude")
	private double longitude;
	@JsonProperty("Latitude")
	private double latitude;
	
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
	
}