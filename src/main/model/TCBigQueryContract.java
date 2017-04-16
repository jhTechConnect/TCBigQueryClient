package main.model;

public class TCBigQueryContract {
	
	public static class SessionDataEntry {
	
		//Will need to modify based on the data window that we're interested
		//This is across all users, not for an individual user
		public static final String END_EARLY_QUERY = "SELECT User,date(thing1) as Day,ID,Name " +
        		"FROM FLATTEN(FLATTEN(SELECT user_dim.app_info.app_instance_id as User,event_dim.timestamp_micros as thing1, event_dim.params.value.string_value as ID, " +
        		"FROM " + 
        		  "TABLE_DATE_RANGE(org_techconnect_ANDROID.app_events_, TIMESTAMP('2017-03-01'), TIMESTAMP('2017-04-07')) " +
        		  "WHERE event_dim.name = 'session_complete' AND event_dim.params.key = 'item_id', event_dim.params.value),event_dim) t1, " +
        		  "LEFT JOIN " +
        		  "FLATTEN(FLATTEN(SELECT event_dim.timestamp_micros as thing2,event_dim.params.value.string_value as Name, " +
        		  "FROM " +
        		  "TABLE_DATE_RANGE(org_techconnect_ANDROID.app_events_, TIMESTAMP('2017-03-01'), TIMESTAMP('2017-04-07')) " +
        		  "WHERE event_dim.name = 'session_complete' AND event_dim.params.key = 'item_name', event_dim.params.value),event_dim) t2 " +
        		  "On t1.thing1 = t2.thing2 " +
        		  "ORDER BY thing1;";
	}
	
	public static class UserDataEntry {
		
		public static final String UNIQUE_USER_QUERY = "";
	}
}
