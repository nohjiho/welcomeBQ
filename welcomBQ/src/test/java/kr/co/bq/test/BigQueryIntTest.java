package kr.co.bq.test;

import org.junit.Test;

import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQueryOptions;
import com.google.cloud.bigquery.Dataset;
import com.google.cloud.bigquery.DatasetInfo;

public class BigQueryIntTest {

	@Test
	public void test() {
		//fail("Not yet implemented");
		System.out.println("Hello Junit!!");
	}
	
	@Test
	public void testBigQueryQuickStart() {
		//fail("Not yet implemented");
		BigQuery bigquery = BigQueryOptions.getDefaultInstance().getService();
		
		String datasetName = "welcomeDW2";
		
		Dataset dataset = null;
		DatasetInfo datasetInfo = DatasetInfo.newBuilder(datasetName).build();
		dataset = bigquery.create(datasetInfo);
		System.out.println("Dataset created : " + dataset.getDatasetId().getDataset());
		
	}

}
