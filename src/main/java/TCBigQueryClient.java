package main.java;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.TimeoutException;

//This is a test to see how Buffer handles git commands
//Further testing

import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQueryOptions;
import main.model.User;
import main.java.Queries;

public class TCBigQueryClient {
	
	private static String EXPORT_DIRECTORY = "";
	
	public static void main(String[] args) throws IOException, FileNotFoundException, InterruptedException, TimeoutException {
		boolean output = false;
		for (int i = 0; i < args.length; i++) {
			if (args[i].equalsIgnoreCase("-o")) {
				EXPORT_DIRECTORY = args[i+1];
				output = true;
			}
		}
		
		if (!output) {
			System.out.println("Output file not provided. Query not completed");
			System.exit(0);
		}
		Queries.setExportDirectory(EXPORT_DIRECTORY);
		BigQuery bigquery = BigQueryOptions.getDefaultInstance().getService();
		
		
		//Complete all population-based metrics for the study
		//Write all session events to CSV file
	    Queries.writeAllSessionEvents(bigquery,"2017-06-01","2017-06-30");
	   
	    //Complete all individual-based data
		//Get list of unique users
		List<User> users = Queries.getUserList(bigquery,"2017-06-10","2017-06-21");
		
		//Use ids to get all info related to individual technician
		for (User u : users) {
			System.out.println(u.getId());
			Queries.writeAllUserSessionEvents(bigquery,u.getId(),"2017-06-01","2017-06-11");
			Queries.writeUserUsageData(bigquery,u.getId(),"2017-06-10","2017-06-21");
		}
		

		//Get comments posted
		Queries.writeCommentData(bigquery,"2017-01-01","2017-06-11", false);
		
	}
}
