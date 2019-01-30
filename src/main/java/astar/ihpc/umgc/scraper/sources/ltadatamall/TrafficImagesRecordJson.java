package astar.ihpc.umgc.scraper.sources.ltadatamall;

import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * The record for traffic images. Some info about the feed and an (expiring) link to download.
 * @author othmannb
 *
 */
public class TrafficImagesRecordJson {
	/*
	 * [{"CameraID":"1001","Latitude":1.29531332,"Longitude":103.871146,"ImageLink":"https://s3-ap-southeast-1.amazonaws.com/mtpdm/2019-01-07/09-26/1001_0923_20190107092407_b8baa4.jpg"}
	 */
	@JsonProperty("CameraID")
	private String cameraId;
	@JsonProperty("Latitude")
	private double latitude;
	@JsonProperty("Longitude")
	private double longitude;
	@JsonProperty("ImageLink")
	private String imageLink;
	public String getCameraId() {
		return cameraId;
	}
	public void setCameraId(String cameraId) {
		this.cameraId = cameraId;
	}
	public double getLatitude() {
		return latitude;
	}
	public void setLatitude(double latitude) {
		this.latitude = latitude;
	}
	public double getLongitude() {
		return longitude;
	}
	public void setLongitude(double longitude) {
		this.longitude = longitude;
	}
	public String getImageLink() {
		return imageLink;
	}
	public void setImageLink(String imageLink) {
		this.imageLink = imageLink;
	}
	@Override
	public String toString() {
		return "TrafficImagesRecordJson [cameraId=" + cameraId + ", latitude=" + latitude + ", longitude=" + longitude
				+ ", imageLink=" + imageLink + "]";
	}
	
	
	

}
