package astar.ihpc.umgc.scraper.sources.ltadatamall;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.fasterxml.jackson.databind.ser.std.ToStringSerializer;

/**
 * The record for Speed Band.
 * @author othmannb
 *
 */
public class SpeedBandRecordJson {
	@JsonProperty(value="LinkID")
	@JsonSerialize(using = ToStringSerializer.class)
	private long linkId;

	@JsonProperty(value="RoadName")
	private String roadName;

	@JsonProperty(value="RoadCategory")
	private char roadCategory;

	@JsonProperty(value="SpeedBand")
	private int speedBand;

	@JsonProperty(value="MinimumSpeed")
	private int minimumSpeed;

	@JsonProperty(value="MaximumSpeed")
	private int maximumSpeed;

	@JsonProperty(value="Location")
	private String location;

	
	public long getLinkId() {
		return linkId;
	}

	public void setLinkId(long linkId) {
		this.linkId = linkId;
	}

	public String getRoadName() {
		return roadName;
	}

	public void setRoadName(String roadName) {
		this.roadName = roadName;
	}

	public char getRoadCategory() {
		return roadCategory;
	}

	public void setRoadCategory(char roadCategory) {
		this.roadCategory = roadCategory;
	}

	public int getSpeedBand() {
		return speedBand;
	}

	public void setSpeedBand(int speedBand) {
		this.speedBand = speedBand;
	}

	public int getMinimumSpeed() {
		return minimumSpeed;
	}

	public void setMinimumSpeed(int minimumSpeed) {
		this.minimumSpeed = minimumSpeed;
	}

	public int getMaximumSpeed() {
		return maximumSpeed;
	}

	public void setMaximumSpeed(int maximumSpeed) {
		this.maximumSpeed = maximumSpeed;
	}

	public String getLocation() {
		return location;
	}

	public void setLocation(String location) {
		this.location = location;
	}

	@Override
	public String toString() {
		return "SpeedBandRecordJson [linkId=" + linkId + ", roadName=" + roadName + ", roadCategory=" + roadCategory
				+ ", speedBand=" + speedBand + ", minimumSpeed=" + minimumSpeed + ", maximumSpeed=" + maximumSpeed
				+ ", location=" + location + "]";
	}
	
	

}