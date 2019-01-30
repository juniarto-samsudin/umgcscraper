package astar.ihpc.umgc.scraper.util;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.time.Instant;
import java.time.OffsetDateTime;

import com.fasterxml.jackson.annotation.JsonFormat;
import com.fasterxml.jackson.annotation.JsonProperty;

public class ScraperRecordMetadata {
	@JsonProperty("file_path")
	private String filePath;
	@JsonProperty("file_dir")
	private String fileDir;
	@JsonProperty("file_name")
	private String fileName;
	@JsonFormat(pattern="yyyy.MM.dd.HH.mm.ss", timezone="Asia/Singapore")
	@JsonProperty("request_timestamp")
	private OffsetDateTime requestTimestamp;
	@JsonProperty("file_hash")
	private String fileHash;
	@JsonProperty("scraper_id")
	private String scraperId;
	@JsonProperty("priority")
	private int priority;
	@JsonProperty("file_length")
	long fileLength;
	
	public ScraperRecordMetadata() {
	}
	
	public ScraperRecordMetadata(String filePath, byte[] data, String scraperId, int priority, long timeMillis) throws IOException{
		this.filePath = filePath;
		this.fileDir = ScraperUtil.calculateDirNameFromFilePath(filePath);
		this.fileName = ScraperUtil.calculateFileNameFromFilePath(filePath);
		this.scraperId = scraperId;
		this.priority = priority;
		this.requestTimestamp = OffsetDateTime.ofInstant(Instant.ofEpochMilli(timeMillis), ScraperUtil.STANDARD_ZONE);
		this.fileHash = ScraperUtil.calculateHash(data);
		this.fileLength = data.length;
	}
	
	public ScraperRecordMetadata(String filePath, String scraperId, int priority, long timeMillis) throws IOException{
		this.filePath = filePath;
		this.fileDir = ScraperUtil.calculateDirNameFromFilePath(filePath);
		this.fileName = ScraperUtil.calculateFileNameFromFilePath(filePath);
		this.scraperId = scraperId;
		this.priority = priority;
		this.requestTimestamp = OffsetDateTime.ofInstant(Instant.ofEpochMilli(timeMillis), ScraperUtil.STANDARD_ZONE);
		File file = new File(filePath);
		this.fileHash = ScraperUtil.calculateHash(Files.readAllBytes(file.toPath()));
		this.fileLength = file.length();
	}

	
	public String getFilePath() {
		return filePath;
	}

	public void setFilePath(String filePath) {
		this.filePath = filePath;
	}

	public String getFileDir() {
		return fileDir;
	}

	public void setFileDir(String fileDir) {
		this.fileDir = fileDir;
	}

	public String getFileName() {
		return fileName;
	}

	public void setFileName(String fileName) {
		this.fileName = fileName;
	}

	public OffsetDateTime getRequestTimestamp() {
		return requestTimestamp;
	}

	public void setRequestTimestamp(OffsetDateTime requestTimestamp) {
		this.requestTimestamp = requestTimestamp;
	}

	public String getFileHash() {
		return fileHash;
	}

	public void setFileHash(String fileHash) {
		this.fileHash = fileHash;
	}

	public String getScraperId() {
		return scraperId;
	}

	public void setScraperId(String scraperId) {
		this.scraperId = scraperId;
	}

	public long getFileLength() {
		return fileLength;
	}

	public void setFileLength(long fileLength) {
		this.fileLength = fileLength;
	}

	public int getPriority() {
		return priority;
	}

	public void setPriority(int priority) {
		this.priority = priority;
	}

	@Override
	public String toString() {
		return "ScraperRecordMetadata [filePath=" + filePath + ", fileDir=" + fileDir + ", fileName=" + fileName
				+ ", requestTimestamp=" + requestTimestamp + ", fileHash=" + fileHash + ", scraperId=" + scraperId
				+ ", priority=" + priority + ", fileLength=" + fileLength + "]";
	}

	
}
