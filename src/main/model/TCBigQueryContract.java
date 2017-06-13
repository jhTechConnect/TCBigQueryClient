package main.model;

public class TCBigQueryContract {
	
	public static class SessionDataEntry {
	
		
		public static final String ALL_EVENTS_DAILY_COUNT_INDIVIDUAL = "SELECT Day, COALESCE(INTEGER(count7),0) as BeginCount, PausedCount, DeleteCount, ResumeCount, EarlyCount, CompleteCount "+
				"FROM (SELECT Day, COALESCE(INTEGER(count6),0) as PausedCount, DeleteCount,ResumeCount,EarlyCount,CompleteCount " +
				  "FROM (SELECT Day, COALESCE(INTEGER(count5),0) as DeleteCount, ResumeCount, EarlyCount, CompleteCount " +
				    "FROM (SELECT Day, COALESCE(INTEGER(count4),0) as ResumeCount, EarlyCount, CompleteCount " +
				      "FROM (SELECT Day , COALESCE(INTEGER(count3),0) as EarlyCount, CompleteCount " +
				        "FROM (SELECT Day, COALESCE(INTEGER(count1),0) as CompleteCount " +
				              "FROM (SELECT date(day) as Day FROM org_techconnect_ANDROID.date_range) t2 " +
				              "LEFT JOIN " +
				              "(SELECT date(event_dim.timestamp_micros) as day1, count(*) as count1," +
				              "FROM " +
				              "TABLE_DATE_RANGE(org_techconnect_ANDROID.app_events_, TIMESTAMP('%s'), TIMESTAMP('%s')) " +
				              "WHERE user_dim.app_info.app_instance_id = '%s' AND event_dim.name = 'session_complete' AND event_dim.params.key = 'item_id' " +
				              "GROUP BY day1 " +
				              "Order BY day1) t1 " +
				              "ON t2.Day = t1.day1) joined " +
				            "LEFT JOIN " +
				            "(SELECT date(event_dim.timestamp_micros) as day3, count(*) as count3, " +
				            "FROM " +
				            "TABLE_DATE_RANGE(org_techconnect_ANDROID.app_events_, TIMESTAMP('%s'), TIMESTAMP('%s')) " +
				            "WHERE user_dim.app_info.app_instance_id = '%s' AND event_dim.name = 'session_end_early_nosave' AND event_dim.params.key = 'item_id' " +
				            "GROUP BY day3 " +
				            "Order BY day3) t3 " +
				            "ON joined.Day= t3.day3) joined2 " +
				        "LEFT JOIN " +
				        "(SELECT date(event_dim.timestamp_micros) as day4, count(*) as count4, " +
				          "FROM  " +
				          "TABLE_DATE_RANGE(org_techconnect_ANDROID.app_events_, TIMESTAMP('%s'), TIMESTAMP('%s')) " +
				          "WHERE user_dim.app_info.app_instance_id = '%s' AND event_dim.name = 'session_resumed' AND event_dim.params.key = 'item_id' " +
				          "GROUP BY day4 " +
				          "Order BY day4) t4 " +
				          "ON joined2.Day = t4.day4 ) joined3 " +
				      "LEFT JOIN " +
				      "(SELECT date(event_dim.timestamp_micros) as day5, count(*) as count5, " +
				        "FROM " +
				        "TABLE_DATE_RANGE(org_techconnect_ANDROID.app_events_, TIMESTAMP('%s'), TIMESTAMP('%s')) " +
				        "WHERE user_dim.app_info.app_instance_id = '%s' AND event_dim.name = 'session_deleted' AND event_dim.params.key = 'item_id' " +
				        "GROUP BY day5 " +
				        "Order BY day5) t5 " +
				        "ON joined3.Day = t5.day5) joined4 " +
				    "LEFT JOIN " +
				    "(SELECT date(event_dim.timestamp_micros) as day6, count(*) as count6, " +
				        "FROM " + 
				        "TABLE_DATE_RANGE(org_techconnect_ANDROID.app_events_, TIMESTAMP('%s'), TIMESTAMP('%s')) " +
				        "WHERE user_dim.app_info.app_instance_id = '%s' AND event_dim.name = 'session_paused' AND event_dim.params.key = 'item_id' " +
				        "GROUP BY day6 " +
				        "Order BY day6) t6 " +
				        "ON joined4.Day = t6.day6) joined5 " +
				  "LEFT JOIN " +
				  "(SELECT date(event_dim.timestamp_micros) as day7, count(*) as count7, " +
				        "FROM " +
				        "TABLE_DATE_RANGE(org_techconnect_ANDROID.app_events_, TIMESTAMP('%s'), TIMESTAMP('%s'))" +
				        "WHERE user_dim.app_info.app_instance_id = '%s' AND event_dim.name = 'session_begin' AND event_dim.params.key = 'item_id' " +
				        "GROUP BY day7 " +
				        "Order BY day7) t7 " +
				        "ON joined5.Day = t7.day7";
		
		public static final String ALL_EVENTS_DAILY_COUNT = "SELECT Day, COALESCE(INTEGER(count7),0) as BeginCount, PausedCount, DeleteCount, ResumeCount, EarlyCount, CompleteCount "+
				"FROM (SELECT Day, COALESCE(INTEGER(count6),0) as PausedCount, DeleteCount,ResumeCount,EarlyCount,CompleteCount " +
				  "FROM (SELECT Day, COALESCE(INTEGER(count5),0) as DeleteCount, ResumeCount, EarlyCount, CompleteCount " +
				    "FROM (SELECT Day, COALESCE(INTEGER(count4),0) as ResumeCount, EarlyCount, CompleteCount " +
				      "FROM (SELECT Day , COALESCE(INTEGER(count3),0) as EarlyCount, CompleteCount " +
				        "FROM (SELECT Day, COALESCE(INTEGER(count1),0) as CompleteCount " +
				              "FROM (SELECT date(day) as Day FROM org_techconnect_ANDROID.date_range) t2 " +
				              "LEFT JOIN " +
				              "(SELECT date(event_dim.timestamp_micros) as day1, count(*) as count1," +
				              "FROM " +
				              "TABLE_DATE_RANGE(org_techconnect_ANDROID.app_events_, TIMESTAMP('%s'), TIMESTAMP('%s')) " +
				              "WHERE event_dim.name = 'session_complete' AND event_dim.params.key = 'item_id' " +
				              "GROUP BY day1 " +
				              "Order BY day1) t1 " +
				              "ON t2.Day = t1.day1) joined " +
				            "LEFT JOIN " +
				            "(SELECT date(event_dim.timestamp_micros) as day3, count(*) as count3, " +
				            "FROM " +
				            "TABLE_DATE_RANGE(org_techconnect_ANDROID.app_events_, TIMESTAMP('%s'), TIMESTAMP('%s')) " +
				            "WHERE event_dim.name = 'session_end_early_nosave' AND event_dim.params.key = 'item_id' " +
				            "GROUP BY day3 " +
				            "Order BY day3) t3 " +
				            "ON joined.Day= t3.day3) joined2 " +
				        "LEFT JOIN " +
				        "(SELECT date(event_dim.timestamp_micros) as day4, count(*) as count4, " +
				          "FROM  " +
				          "TABLE_DATE_RANGE(org_techconnect_ANDROID.app_events_, TIMESTAMP('%s'), TIMESTAMP('%s')) " +
				          "WHERE event_dim.name = 'session_resumed' AND event_dim.params.key = 'item_id' " +
				          "GROUP BY day4 " +
				          "Order BY day4) t4 " +
				          "ON joined2.Day = t4.day4 ) joined3 " +
				      "LEFT JOIN " +
				      "(SELECT date(event_dim.timestamp_micros) as day5, count(*) as count5, " +
				        "FROM " +
				        "TABLE_DATE_RANGE(org_techconnect_ANDROID.app_events_, TIMESTAMP('%s'), TIMESTAMP('%s')) " +
				        "WHERE event_dim.name = 'session_deleted' AND event_dim.params.key = 'item_id' " +
				        "GROUP BY day5 " +
				        "Order BY day5) t5 " +
				        "ON joined3.Day = t5.day5) joined4 " +
				    "LEFT JOIN " +
				    "(SELECT date(event_dim.timestamp_micros) as day6, count(*) as count6, " +
				        "FROM " + 
				        "TABLE_DATE_RANGE(org_techconnect_ANDROID.app_events_, TIMESTAMP('%s'), TIMESTAMP('%s')) " +
				        "WHERE event_dim.name = 'session_paused' AND event_dim.params.key = 'item_id' " +
				        "GROUP BY day6 " +
				        "Order BY day6) t6 " +
				        "ON joined4.Day = t6.day6) joined5 " +
				  "LEFT JOIN " +
				  "(SELECT date(event_dim.timestamp_micros) as day7, count(*) as count7, " +
				        "FROM " +
				        "TABLE_DATE_RANGE(org_techconnect_ANDROID.app_events_, TIMESTAMP('%s'), TIMESTAMP('%s'))" +
				        "WHERE event_dim.name = 'session_begin' AND event_dim.params.key = 'item_id' " +
				        "GROUP BY day7 " +
				        "Order BY day7) t7 " +
				        "ON joined5.Day = t7.day7";
	}
	
	public static class DateHandlingEntry {
		//Use this command first to define the desired date range that will be used for the other queries
		//that depend on a date range for the calcualtions
		public static final String SET_DATE_RANGE = "SELECT date(event_dim.timestamp_micros) as day," +
				  "FROM " +
				  "TABLE_DATE_RANGE(org_techconnect_ANDROID.app_events_, TIMESTAMP('%s'), TIMESTAMP('%s')) " +
				  "GROUP BY day " +
				  "ORDER BY day ";
		
	}
	public static class GeneralInfoEntry {
		
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
		public static final String COMMENTS_COUNT = "SELECT date(event_dim.timestamp_micros) as day, count(*) as Count, " +
				  "FROM " +
				  "TABLE_DATE_RANGE(org_techconnect_ANDROID.app_events_, TIMESTAMP('%s'), TIMESTAMP('%s')) " +
				  "WHERE event_dim.name = 'post_comment' " +
				  "GROUP BY day " +
				  "Order BY day";
		
		public static final String ALL_COMMENT_DATA = "SELECT date(event_dim.timestamp_micros) as day, user_dim.app_info.app_instance_id as User, event_dim.params.value.string_value as Flowchart" +
		  "FROM " +
		  "TABLE_DATE_RANGE(org_techconnect_ANDROID.app_events_, TIMESTAMP('%s'), TIMESTAMP('%s')) "+
		  "WHERE event_dim.name = 'post_comment' AND event_dim.params.key = 'item_name' " +
		  "Order BY day";
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
