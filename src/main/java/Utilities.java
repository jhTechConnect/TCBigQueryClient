package main.java;

import java.util.UUID;
import java.util.concurrent.TimeoutException;

import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.Job;
import com.google.cloud.bigquery.JobId;
import com.google.cloud.bigquery.JobInfo;
import com.google.cloud.bigquery.QueryJobConfiguration;
import com.google.cloud.bigquery.QueryResponse;
import com.google.cloud.bigquery.QueryResult;
import com.google.cloud.bigquery.TableId;
import com.google.cloud.bigquery.JobInfo.WriteDisposition;

public class Utilities {
	
	/**
	 * Wrapper for all of the code needed to actually execute a query with BigQuery
	 * @param bq
	 * @param query
	 * @return
	 * @throws TimeoutException
	 * @throws InterruptedException
	 */
	public static QueryResult completeQuery(BigQuery bq, String query) throws TimeoutException, InterruptedException  {
		QueryJobConfiguration queryConfig =
		        QueryJobConfiguration.newBuilder(query)
	            .setUseLegacySql(true) //This is whatever I've been using online, so I'm going to stick with it
	            .build();

		    // Create a job ID so that we can safely retry.
		    JobId jobId = JobId.of(UUID.randomUUID().toString());
		    Job queryJob = bq.create(JobInfo.newBuilder(queryConfig).setJobId(jobId).build());

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
		    QueryResponse response = bq.getQueryResults(jobId);
		    return response.getResult();
	}
	
	public static QueryResult completeQueryOverwriteTable(BigQuery bq, String query, String tablename) throws TimeoutException, InterruptedException  {
		QueryJobConfiguration queryConfig =
		        QueryJobConfiguration.newBuilder(query)
		        .setDestinationTable(TableId.of("org_techconnect_ANDROID", tablename))
		        .setWriteDisposition(WriteDisposition.WRITE_TRUNCATE)
	            .setUseLegacySql(true) //This is whatever I've been using online, so I'm going to stick with it
	            .build();

		    // Create a job ID so that we can safely retry.
		    JobId jobId = JobId.of(UUID.randomUUID().toString());
		    Job queryJob = bq.create(JobInfo.newBuilder(queryConfig).setJobId(jobId).build());

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
		    QueryResponse response = bq.getQueryResults(jobId);
		    return response.getResult();
	}

}
