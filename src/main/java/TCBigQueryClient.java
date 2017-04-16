package main.java;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeoutException;

import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQueryOptions;
import com.google.cloud.bigquery.Field;
import com.google.cloud.bigquery.FieldValue;
import com.google.cloud.bigquery.Job;
import com.google.cloud.bigquery.JobId;
import com.google.cloud.bigquery.JobInfo;
import com.google.cloud.bigquery.QueryJobConfiguration;
import com.google.cloud.bigquery.QueryResponse;
import com.google.cloud.bigquery.QueryResult;


public class TCBigQueryClient {
	
	
	public static void main(String[] args) throws IOException, FileNotFoundException, InterruptedException, TimeoutException {
		BigQuery bigquery = BigQueryOptions.getDefaultInstance().getService();
	    // [END create_client]
	    // [START run_query]
	    
	    QueryJobConfiguration queryConfig =
	        QueryJobConfiguration.newBuilder(
	        		"SELECT User,date(thing1) as Day,ID,Name " +
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
	        		  "ORDER BY thing1;")
	            .setUseLegacySql(true) //This is whatever I've been using online, so I'm going to stick with it
	            .build();

	    // Create a job ID so that we can safely retry.
	    JobId jobId = JobId.of(UUID.randomUUID().toString());
	    Job queryJob = bigquery.create(JobInfo.newBuilder(queryConfig).setJobId(jobId).build());

	    // Wait for the query to complete.
	    queryJob = queryJob.waitFor();

	    // Check for errors
	    if (queryJob == null) {
	      throw new RuntimeException("Job no longer exists");
	    } else if (queryJob.getStatus().getError() != null) {
	      // You can also look at queryJob.getStatus().getExecutionErrors() for all
	      // errors, not just the latest one.
	      throw new RuntimeException(queryJob.getStatus().getError().toString());
	    }

	    // Get the results.
	    QueryResponse response = bigquery.getQueryResults(jobId);
	    QueryResult result = response.getResult();
	    List<Field> fields = result.getSchema().getFields();
	    
	    //Get the column names of the result
	    for (Field f : fields) {
	    	System.out.print(String.format("%s,",f.getName()));
	    }
	    System.out.println();
	    
	    //Also from the sample application, hoping that this prints everything out nicely?
	    while (result != null) {
	    	  Iterator<List<FieldValue>> iter = result.iterateAll();
	    	  while (iter.hasNext()) {
	    	    List<FieldValue> row = iter.next();
	    	    
	    	    for (FieldValue f : row) {
	    	    	System.out.print(String.format("%s,",f.getStringValue()));
	    	    }
	    	    System.out.println();
	    	  }
	    	  result = result.getNextPage();
	    }
	}
}
