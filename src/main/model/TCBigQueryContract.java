package main.model;

public class TCBigQueryContract {
	
	public static class SessionDataEntry {
	
		//Will need to modify based on the data window that we're interested
		//This is across all users, not for an individual user
		public static final String COMPLETE_QUERY = "SELECT User,date(thing1) as Day,ID,Name " +
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
		
		public static final String COMPLETE_QUERY_INDIVIDUAL = "SELECT User,date(thing1) as Day,ID,Name " +
        		"FROM FLATTEN(FLATTEN(SELECT user_dim.app_info.app_instance_id as User,event_dim.timestamp_micros as thing1, event_dim.params.value.string_value as ID, " +
        		"FROM " + 
        		  "TABLE_DATE_RANGE(org_techconnect_ANDROID.app_events_, TIMESTAMP('2017-03-01'), TIMESTAMP('2017-04-07')) " +
        		  "WHERE user_dim.app_info.app_instance_id = '%s' AND event_dim.name = 'session_complete' AND event_dim.params.key = 'item_id', event_dim.params.value),event_dim) t1, " +
        		  "LEFT JOIN " +
        		  "FLATTEN(FLATTEN(SELECT event_dim.timestamp_micros as thing2,event_dim.params.value.string_value as Name, " +
        		  "FROM " +
        		  "TABLE_DATE_RANGE(org_techconnect_ANDROID.app_events_, TIMESTAMP('2017-03-01'), TIMESTAMP('2017-04-07')) " +
        		  "WHERE user_dim.app_info.app_instance_id = '%s' AND event_dim.name = 'session_complete' AND event_dim.params.key = 'item_name', event_dim.params.value),event_dim) t2 " +
        		  "On t1.thing1 = t2.thing2 " +
        		  "ORDER BY thing1;";
		
		public static final String ENDEARLY_QUERY_INDIVIDUAL = "SELECT date(thing1) as Day,ID,Name " +
			"FROM FLATTEN(FLATTEN(SELECT event_dim.timestamp_micros as thing1, event_dim.params.value.string_value as ID, " +
			"FROM " +
			"TABLE_DATE_RANGE(org_techconnect_ANDROID.app_events_, TIMESTAMP('%s'), TIMESTAMP('%s')) " +
			"WHERE user_dim.app_info.app_instance_id = '%s' AND event_dim.name = 'session_end_early' AND event_dim.params.key = 'item_id', event_dim.params.value),event_dim) t1, " +
			"LEFT JOIN " +
			"FLATTEN(FLATTEN(SELECT event_dim.timestamp_micros as thing2,event_dim.params.value.string_value as Name, " +
			"FROM " + 
			"TABLE_DATE_RANGE(org_techconnect_ANDROID.app_events_, TIMESTAMP('%s'), TIMESTAMP('%s')) " +
			"WHERE user_dim.app_info.app_instance_id = '%s' AND event_dim.name = 'session_end_early' AND event_dim.params.key = 'item_name', event_dim.params.value),event_dim) t2 " +
			"On t1.thing1 = t2.thing2 " +
			"ORDER BY thing1";
		
		public static final String COMPLETE_DAILY_COUNT = "SELECT date(event_dim.timestamp_micros) as day, count(*) as Count, " +
			"FROM " +
			"TABLE_DATE_RANGE(org_techconnect_ANDROID.app_events_, TIMESTAMP('2017-03-01'), TIMESTAMP('2017-04-07')) " +
			"WHERE event_dim.name = 'session_complete' AND event_dim.params.key = 'item_id' " +
			"GROUP BY day " +
			"ORDER BY day";	
		
		public static final String ALL_EVENTS_DAILY_COUNT = "SELECT day2 as Day ,COALESCE(INTEGER(count3),0) as EarlyCount, CompleteCount, " +
				"FROM " +
				"(SELECT day2, COALESCE(INTEGER(count1),0) as CompleteCount " +
				    "FROM (SELECT date(day) as day2 FROM org_techconnect_ANDROID.date_range) t2 " +
				    "LEFT JOIN " +
				   "(SELECT date(event_dim.timestamp_micros) as day1, count(*) as count1, " +
				    "FROM " + 
				    "TABLE_DATE_RANGE(org_techconnect_ANDROID.app_events_, TIMESTAMP('%s'), TIMESTAMP('%s')) " +
				    "WHERE event_dim.name = 'session_complete' AND event_dim.params.key = 'item_id' " +
				    "GROUP BY day1 " +
				    "Order BY day1) t1 " +
				    "ON t1.day1 = t2.day2) joined " +
				  "LEFT JOIN " +
				  "(SELECT date(event_dim.timestamp_micros) as day3, count(*) as count3, " +
				  "FROM " + 
				  "TABLE_DATE_RANGE(org_techconnect_ANDROID.app_events_, TIMESTAMP('%s'), TIMESTAMP('%s')) " +
				  "WHERE event_dim.name = 'session_end_early' AND event_dim.params.key = 'item_id' " +
				  "GROUP BY day3 " +
				  "Order BY day3) t3 " +
				  "ON joined.day2 = t3.day3";
	}
	
	public static class GeneralInfoEntry {
		
		public static final String ALL_USER_APP_SESSIONS = "SELECT event_dim.name as Event, Date(event_dim.timestamp_micros) as Day,event_dim.timestamp_micros as DayMicros " + 
		  "FROM " + 
		  "TABLE_DATE_RANGE(org_techconnect_ANDROID.app_events_, TIMESTAMP('%s'), TIMESTAMP('%s')) " +
		  "WHERE user_dim.app_info.app_instance_id = '%s' AND event_dim.name = 'session_start' " +
		  "ORDER BY DayMicros";
		
		public static final String ALL_USER_INFO_SESSIONS = "SELECT Event, date(thing1) as Date, thing1 as Raw, Class, EngagementTime FROM " +
				  "FLATTEN(FLATTEN(SELECT event_dim.name as Event, event_dim.timestamp_micros as thing1, event_dim.params.value.string_value as Class, " +
						  "FROM " +
						  "TABLE_DATE_RANGE(org_techconnect_ANDROID.app_events_, TIMESTAMP('%s'), TIMESTAMP('%s')) " +
						  "WHERE user_dim.app_info.app_instance_id = '%s'AND (event_dim.name = 'user_engagement' OR event_dim.name = 'session_start') " +
						  "AND event_dim.params.key = 'firebase_screen_class', event_dim.params.value),event_dim) t1, " +
						  "LEFT JOIN " +
						  "FLATTEN(FLATTEN(SELECT event_dim.timestamp_micros as thing2,event_dim.params.value.int_value as EngagementTime, " +
						  "FROM " +
						  "TABLE_DATE_RANGE(org_techconnect_ANDROID.app_events_, TIMESTAMP('%s'), TIMESTAMP('%s')) " +
						  "WHERE user_dim.app_info.app_instance_id = '%s' AND event_dim.name = 'user_engagement' " +
						  "AND event_dim.params.key = 'engagement_time_msec', event_dim.params.value),event_dim) t2 " +
						  "On t1.thing1 = t2.thing2 "+
						  "ORDER BY thing1";
	}
	
	public static class UserDataEntry {
		
		//Use this info to get a list of the unique user IDs. This can then be used to generate specific metrics for each user
		public static final String LIST_UNIQUE_USER = "SELECT user_dim.app_info.app_instance_id as User, user_dim.device_info. " + 
		"mobile_marketing_name as DeviceName, user_dim.device_info.platform_version as Version " +
		"FROM " +
		"TABLE_DATE_RANGE(org_techconnect_ANDROID.app_events_, TIMESTAMP('%s'), TIMESTAMP('%s')) " +
		"GROUP BY User, DeviceName, Version";
	}
}
