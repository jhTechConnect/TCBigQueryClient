package main.java;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.ArrayList;
import java.util.UUID;
import java.util.concurrent.TimeoutException;
import java.io.File;
import java.io.FileWriter;

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
import com.opencsv.CSVWriter;

import main.model.TCBigQueryContract;

public class TCBigQueryClient {
	
	public final static String EXPORT_DIRECTORY = "/Users/doranwalsten/Documents/CBID/TechConnect/Usage/";
	
	
	public static void main(String[] args) throws IOException, FileNotFoundException, InterruptedException, TimeoutException {
		BigQuery bigquery = BigQueryOptions.getDefaultInstance().getService();
	    // [END create_client]
	    // [START run_query]
	    
	    QueryJobConfiguration queryConfig =
	        QueryJobConfiguration.newBuilder(TCBigQueryContract.SessionDataEntry.COMPLETE_QUERY)
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
	    
	    //Prepare CSVWriter in order to write output
	    File file = new File(EXPORT_DIRECTORY, "EndEarly.csv");
	    CSVWriter writer = new CSVWriter(new FileWriter(file));
	    List<Field> fields = result.getSchema().getFields();
	    
	    //Get the column names of the result
	    ArrayList<String> dataRow = new ArrayList<String>();
	    for (Field f : fields) {
	    	dataRow.add(f.getName());
	    }
	    String[] dataRowArray = new String[dataRow.size()];
	    dataRowArray = dataRow.toArray(dataRowArray);
	    writer.writeNext(dataRowArray);
	    dataRow.clear();
	    
	    while (result != null) {
	    	  Iterator<List<FieldValue>> iter = result.iterateAll();
	    	  while (iter.hasNext()) {
	    	    List<FieldValue> row = iter.next();
	    	    
	    	    for (FieldValue f : row) {
	    	    	dataRow.add(f.getStringValue());
	    	    }
	    	    dataRowArray = new String[dataRow.size()];
			    dataRowArray = dataRow.toArray(dataRowArray);
			    writer.writeNext(dataRowArray);
			    dataRow.clear();
	    	  }
	    	  result = result.getNextPage();
	    }
	    
	    writer.close();
	}
}
