package main.model;

/**
 * This class is used to store data relevant to our users stored in BigQuery
 * @author doranwalsten
 *
 */
public class User {
	String id;
	String device;
	String version;
	
	public User(String id, String device, String version) {
		this.id = id;
		this.device = device;
		this.version = version;
	}
	
	public User() {
		
	}
	
	public String getId() {
		return id;
	}
	
	public void setId(String id) {
		this.id = id;
	}
	
	public String getDevice() {
		return device;
	}
	
	public void setDevice(String device) {
		this.device = device;
	}
	
	public String getVersion() {
		return version;
	}
	
	void setVersion(String version) {
		this.version = version;
	}
	
	

}
