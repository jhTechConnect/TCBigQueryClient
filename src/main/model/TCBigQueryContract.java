package main.model;

public class TCBigQueryContract {
	
	public static class SessionDataEntry {
	
		
		// Saved as ALL_EVENTS_DAILY_COUNT_INDIVIDUAL on BigQuery
		// 9 entries
		public static final String ALL_EVENTS_DAILY_COUNT_INDIVIDUAL = "SELECT Day, COALESCE(INTEGER(count10),0) as PausedStubCount, EarlyStubCount, CompleteStubCount, BeginCount, PausedCount, DeleteCount, ResumeCount, EarlyCount, CompleteCount\n" + 
				"FROM (SELECT Day, COALESCE(INTEGER(count9),0) as EarlyStubCount, CompleteStubCount, BeginCount, PausedCount, DeleteCount, ResumeCount, EarlyCount, CompleteCount\n" + 
				"      FROM (SELECT Day, COALESCE(INTEGER(count8),0) as CompleteStubCount, BeginCount, PausedCount, DeleteCount, ResumeCount, EarlyCount, CompleteCount\n" + 
				"            FROM (SELECT Day, COALESCE(INTEGER(count7),0) as BeginCount, PausedCount, DeleteCount, ResumeCount, EarlyCount, CompleteCount\n" + 
				"                  FROM (SELECT Day, COALESCE(INTEGER(count6),0) as PausedCount, DeleteCount,ResumeCount,EarlyCount,CompleteCount\n" + 
				"                        FROM (SELECT Day, COALESCE(INTEGER(count5),0) as DeleteCount, ResumeCount, EarlyCount, CompleteCount\n" + 
				"                              FROM (SELECT Day, COALESCE(INTEGER(count4),0) as ResumeCount, EarlyCount, CompleteCount\n" + 
				"                                    FROM (SELECT Day , COALESCE(INTEGER(count3),0) as EarlyCount, CompleteCount\n" + 
				"                                          FROM (SELECT Day, COALESCE(INTEGER(count1),0) as CompleteCount\n" + 
				"                                                FROM (SELECT date(day) as Day FROM org_techconnect_ANDROID.date_range) t2\n" + 
				"                                                    LEFT JOIN\n" + 
				"                                                   (SELECT date(event_dim.timestamp_micros) as day1, count(*) as count1,\n" + 
				"                                                    FROM\n" + 
				"                                                    TABLE_DATE_RANGE(org_techconnect_ANDROID.app_events_, TIMESTAMP('%s'), TIMESTAMP('%s'))\n" + 
				"                                                    WHERE user_dim.app_info.app_instance_id = '%s' AND event_dim.name = 'session_complete' AND event_dim.params.key = 'item_id'\n" + 
				"                                                    GROUP BY day1\n" + 
				"                                                    Order BY day1) t1\n" + 
				"                                                    ON t2.Day = t1.day1) joined\n" + 
				"                                              LEFT JOIN\n" + 
				"                                              (SELECT date(event_dim.timestamp_micros) as day3, count(*) as count3,\n" + 
				"                                              FROM\n" + 
				"                                              TABLE_DATE_RANGE(org_techconnect_ANDROID.app_events_, TIMESTAMP('%s'), TIMESTAMP('%s'))\n" + 
				"                                              WHERE user_dim.app_info.app_instance_id = '%s' AND event_dim.name = 'session_end_early_nosave' AND event_dim.params.key = 'item_id'\n" + 
				"                                              GROUP BY day3\n" + 
				"                                              Order BY day3) t3\n" + 
				"                                              ON joined.Day= t3.day3) joined2\n" + 
				"                                        LEFT JOIN\n" + 
				"                                        (SELECT date(event_dim.timestamp_micros) as day4, count(*) as count4,\n" + 
				"                                        FROM\n" + 
				"                                        TABLE_DATE_RANGE(org_techconnect_ANDROID.app_events_, TIMESTAMP('%s'), TIMESTAMP('%s'))\n" + 
				"                                        WHERE user_dim.app_info.app_instance_id = '%s' AND event_dim.name = 'session_resumed' AND event_dim.params.key = 'item_id'\n" + 
				"                                        GROUP BY day4\n" + 
				"                                        Order BY day4) t4\n" + 
				"                                        ON joined2.Day = t4.day4 ) joined3\n" + 
				"                                  LEFT JOIN\n" + 
				"                                  (SELECT date(event_dim.timestamp_micros) as day5, count(*) as count5,\n" + 
				"                                  FROM\n" + 
				"                                  TABLE_DATE_RANGE(org_techconnect_ANDROID.app_events_, TIMESTAMP('%s'), TIMESTAMP('%s'))\n" + 
				"                                  WHERE user_dim.app_info.app_instance_id = '%s' AND event_dim.name = 'session_deleted' AND event_dim.params.key = 'item_id'\n" + 
				"                                  GROUP BY day5\n" + 
				"                                  Order BY day5) t5\n" + 
				"                                  ON joined3.Day = t5.day5) joined4\n" + 
				"                            LEFT JOIN\n" + 
				"                            (SELECT date(event_dim.timestamp_micros) as day6, count(*) as count6,\n" + 
				"                             FROM\n" + 
				"                             TABLE_DATE_RANGE(org_techconnect_ANDROID.app_events_, TIMESTAMP('%s'), TIMESTAMP('%s'))\n" + 
				"                             WHERE user_dim.app_info.app_instance_id = '%s' AND event_dim.name = 'session_paused' AND event_dim.params.key = 'item_id'\n" + 
				"                             GROUP BY day6\n" + 
				"                             Order BY day6) t6\n" + 
				"                             ON joined4.Day = t6.day6) joined5\n" + 
				"                       LEFT JOIN\n" + 
				"                       (SELECT date(event_dim.timestamp_micros) as day7, count(*) as count7,\n" + 
				"                       FROM\n" + 
				"                       TABLE_DATE_RANGE(org_techconnect_ANDROID.app_events_, TIMESTAMP('%s'), TIMESTAMP('%s'))\n" + 
				"                       WHERE user_dim.app_info.app_instance_id = '%s' AND event_dim.name = 'session_begin' AND event_dim.params.key = 'item_id'\n" + 
				"                       GROUP BY day7\n" + 
				"                       Order BY day7) t7\n" + 
				"                       ON joined5.Day = t7.day7) joined6\n" + 
				"                 LEFT JOIN\n" + 
				"                 (SELECT date(event_dim.timestamp_micros) as day8, count(*) as count8,\n" + 
				"                 FROM\n" + 
				"                 TABLE_DATE_RANGE(org_techconnect_ANDROID.app_events_, TIMESTAMP('%s'), TIMESTAMP('%s'))\n" + 
				"                 WHERE user_dim.app_info.app_instance_id = '%s' AND event_dim.name = 'session_complete_stub' AND event_dim.params.key = 'item_id'\n" + 
				"                 GROUP BY day8\n" + 
				"                 Order BY day8) t8\n" + 
				"                 ON joined6.Day = t8.day8) joined7\n" + 
				"           LEFT JOIN\n" + 
				"           (SELECT date(event_dim.timestamp_micros) as day9, count(*) as count9,\n" + 
				"           FROM\n" + 
				"           TABLE_DATE_RANGE(org_techconnect_ANDROID.app_events_, TIMESTAMP('%s'), TIMESTAMP('%s'))\n" + 
				"           WHERE user_dim.app_info.app_instance_id = '%s' AND event_dim.name = 'session_end_early_nosave_stub' AND event_dim.params.key = 'item_id'\n" + 
				"           GROUP BY day9\n" + 
				"           Order BY day9) t9\n" + 
				"           ON joined7.Day = t9.day9) joined8\n" + 
				"     LEFT JOIN\n" + 
				"     (SELECT date(event_dim.timestamp_micros) as day10, count(*) as count10,\n" + 
				"     FROM\n" + 
				"     TABLE_DATE_RANGE(org_techconnect_ANDROID.app_events_, TIMESTAMP('%s'), TIMESTAMP('%s'))\n" + 
				"     WHERE user_dim.app_info.app_instance_id = '%s' AND event_dim.name = 'session_paused_stub' AND event_dim.params.key = 'item_id'\n" + 
				"     GROUP BY day10\n" + 
				"     Order BY day10) t10\n" + 
				"     ON joined8.Day = t10.day10";
		
		// 9 pairs of string literals
		// ALL_EVENTS_DAILY_COUNT saved query in BigQuery
		public static final String ALL_EVENTS_DAILY_COUNT = "SELECT Day, COALESCE(INTEGER(count10),0) as PausedStubCount, EarlyStubCount, CompleteStubCount, BeginCount, PausedCount, DeleteCount, ResumeCount, EarlyCount, CompleteCount\n" + 
				"  FROM\n" + 
				"  (SELECT Day, COALESCE(INTEGER(count9),0) as EarlyStubCount, CompleteStubCount, BeginCount, PausedCount, DeleteCount, ResumeCount, EarlyCount, CompleteCount\n" + 
				"  FROM\n" + 
				"    (SELECT Day, COALESCE(INTEGER(count8),0) as CompleteStubCount, BeginCount, PausedCount, DeleteCount, ResumeCount, EarlyCount, CompleteCount\n" + 
				"    FROM\n" + 
				"      (SELECT Day, COALESCE(INTEGER(count7),0) as BeginCount, PausedCount, DeleteCount, ResumeCount, EarlyCount, CompleteCount\n" + 
				"      FROM\n" + 
				"        (SELECT Day, COALESCE(INTEGER(count6),0) as PausedCount, DeleteCount,ResumeCount,EarlyCount,CompleteCount\n" + 
				"        FROM\n" + 
				"          (SELECT Day, COALESCE(INTEGER(count5),0) as DeleteCount, ResumeCount, EarlyCount, CompleteCount\n" + 
				"          FROM\n" + 
				"            (SELECT Day, COALESCE(INTEGER(count4),0) as ResumeCount, EarlyCount, CompleteCount\n" + 
				"            FROM\n" + 
				"              (SELECT day2 as Day , COALESCE(INTEGER(count3),0) as EarlyCount, CompleteCount, \n" + 
				"              FROM \n" + 
				"                  (SELECT day2, COALESCE(INTEGER(count1),0) as CompleteCount\n" + 
				"                    FROM (SELECT date(day) as day2 FROM org_techconnect_ANDROID.date_range) t2\n" + 
				"                    LEFT JOIN\n" + 
				"                    (SELECT date(event_dim.timestamp_micros) as day1, count(*) as count1,\n" + 
				"                    FROM \n" + 
				"                    TABLE_DATE_RANGE(org_techconnect_ANDROID.app_events_, TIMESTAMP('%s'), TIMESTAMP('%s'))\n" + 
				"                    WHERE event_dim.name = 'session_complete' AND event_dim.params.key = 'item_id'\n" + 
				"                    GROUP BY day1\n" + 
				"                    Order BY day1) t1\n" + 
				"                    ON t1.day1 = t2.day2) joined\n" + 
				"                  LEFT JOIN\n" + 
				"                  (SELECT date(event_dim.timestamp_micros) as day3, count(*) as count3,\n" + 
				"                  FROM \n" + 
				"                  TABLE_DATE_RANGE(org_techconnect_ANDROID.app_events_, TIMESTAMP('%s'), TIMESTAMP('%s'))\n" + 
				"                  WHERE event_dim.name = 'session_end_early_nosave' AND event_dim.params.key = 'item_id'\n" + 
				"                  GROUP BY day3\n" + 
				"                  Order BY day3) t3\n" + 
				"                  ON joined.day2 = t3.day3) joined2\n" + 
				"              LEFT JOIN\n" + 
				"              (SELECT date(event_dim.timestamp_micros) as day4, count(*) as count4,\n" + 
				"                FROM \n" + 
				"                TABLE_DATE_RANGE(org_techconnect_ANDROID.app_events_, TIMESTAMP('%s'), TIMESTAMP('%s'))\n" + 
				"                WHERE event_dim.name = 'session_resumed' AND event_dim.params.key = 'item_id'\n" + 
				"                GROUP BY day4\n" + 
				"                Order BY day4) t4\n" + 
				"                ON joined2.Day = t4.day4 ) joined3\n" + 
				"            LEFT JOIN\n" + 
				"            (SELECT date(event_dim.timestamp_micros) as day5, count(*) as count5,\n" + 
				"              FROM \n" + 
				"              TABLE_DATE_RANGE(org_techconnect_ANDROID.app_events_, TIMESTAMP('%s'), TIMESTAMP('%s'))\n" + 
				"              WHERE event_dim.name = 'session_deleted' AND event_dim.params.key = 'item_id'\n" + 
				"              GROUP BY day5\n" + 
				"              Order BY day5) t5\n" + 
				"              ON joined3.Day = t5.day5) joined4\n" + 
				"          LEFT JOIN\n" + 
				"          (SELECT date(event_dim.timestamp_micros) as day6, count(*) as count6,\n" + 
				"              FROM \n" + 
				"              TABLE_DATE_RANGE(org_techconnect_ANDROID.app_events_, TIMESTAMP('%s'), TIMESTAMP('%s'))\n" + 
				"              WHERE event_dim.name = 'session_paused' AND event_dim.params.key = 'item_id'\n" + 
				"              GROUP BY day6\n" + 
				"              Order BY day6) t6\n" + 
				"              ON joined4.Day = t6.day6) joined5\n" + 
				"        LEFT JOIN\n" + 
				"        (SELECT date(event_dim.timestamp_micros) as day7, count(*) as count7,\n" + 
				"              FROM \n" + 
				"              TABLE_DATE_RANGE(org_techconnect_ANDROID.app_events_, TIMESTAMP('%s'), TIMESTAMP('%s'))\n" + 
				"              WHERE event_dim.name = 'session_begin' AND event_dim.params.key = 'item_id'\n" + 
				"              GROUP BY day7\n" + 
				"              Order BY day7) t7\n" + 
				"              ON joined5.Day = t7.day7) joined6\n" + 
				"      LEFT JOIN\n" + 
				"      (SELECT date(event_dim.timestamp_micros) as day8, count(*) as count8,\n" + 
				"        FROM\n" + 
				"        TABLE_DATE_RANGE(org_techconnect_ANDROID.app_events_, TIMESTAMP('%s'), TIMESTAMP('%s'))\n" + 
				"        WHERE event_dim.name = 'session_complete_stub' AND event_dim.params.key = 'item_id'\n" + 
				"              GROUP BY day8\n" + 
				"              Order BY day8) t8\n" + 
				"              ON joined6.Day = t8.day8) joined7\n" + 
				"    LEFT JOIN\n" + 
				"    (SELECT date(event_dim.timestamp_micros) as day9, count(*) as count9,\n" + 
				"        FROM\n" + 
				"        TABLE_DATE_RANGE(org_techconnect_ANDROID.app_events_, TIMESTAMP('%s'), TIMESTAMP('%s'))\n" + 
				"        WHERE event_dim.name = 'session_end_early_nosave_stub' AND event_dim.params.key = 'item_id'\n" + 
				"              GROUP BY day9\n" + 
				"              Order BY day9) t9\n" + 
				"              ON joined7.Day = t9.day9) joined8\n" + 
				"  LEFT JOIN\n" + 
				"  (SELECT date(event_dim.timestamp_micros) as day10, count(*) as count10,\n" + 
				"        FROM\n" + 
				"        TABLE_DATE_RANGE(org_techconnect_ANDROID.app_events_, TIMESTAMP('%s'), TIMESTAMP('%s'))\n" + 
				"        WHERE event_dim.name = 'session_paused_stub' AND event_dim.params.key = 'item_id'\n" + 
				"              GROUP BY day10\n" + 
				"              Order BY day10) t10\n" + 
				"              ON joined8.Day = t10.day10";
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
		
		public static final String USER_USAGE_INFO = "SELECT Event, date(thing1) as Date, thing1 as Raw, Class, EngagementTime\n" + 
				"FROM FLATTEN(FLATTEN(SELECT event_dim.name as Event, event_dim.timestamp_micros as thing1, event_dim.params.value.string_value as Class,\n" + 
				"                    FROM\n" + 
				"						        TABLE_DATE_RANGE(org_techconnect_ANDROID.app_events_, TIMESTAMP('%s'), TIMESTAMP('%s'))\n" + 
				"						        WHERE user_dim.app_info.app_instance_id = '%s' AND (event_dim.name = 'user_engagement' OR event_dim.name = 'session_start')\n" + 
				"						        AND event_dim.params.key = 'firebase_screen_class', event_dim.params.value),event_dim) t1,\n" + 
				"              LEFT JOIN\n" + 
				"              FLATTEN(FLATTEN(SELECT event_dim.timestamp_micros as thing2,event_dim.params.value.int_value as EngagementTime,\n" + 
				"              FROM\n" + 
				"              TABLE_DATE_RANGE(org_techconnect_ANDROID.app_events_, TIMESTAMP('%s'), TIMESTAMP('%s'))\n" + 
				"              WHERE user_dim.app_info.app_instance_id = '%s' AND event_dim.name = 'user_engagement'\n" + 
				"              AND event_dim.params.key = 'engagement_time_msec', event_dim.params.value),event_dim) t2\n" + 
				"              On t1.thing1 = t2.thing2\n" + 
				"              ORDER BY thing1";
	}
}
