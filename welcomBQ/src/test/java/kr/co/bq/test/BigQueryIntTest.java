package kr.co.bq.test;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.Test;

import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQueryOptions;
import com.google.cloud.bigquery.Dataset;
import com.google.cloud.bigquery.DatasetInfo;
import com.google.cloud.bigquery.Field;
import com.google.cloud.bigquery.LegacySQLTypeName;
import com.google.cloud.bigquery.Schema;
import com.google.cloud.bigquery.StandardTableDefinition;
import com.google.cloud.bigquery.Table;
import com.google.cloud.bigquery.TableId;
import com.google.cloud.bigquery.TableInfo;

public class BigQueryIntTest {
	private String datasetId = "lab";

	@Test
	public void test() {
		//fail("Not yet implemented");
		boolean flag = true;
		assertThat(flag).isTrue();
		System.out.println("Hello Junit!!");
	}
	
	/**
	 * 데이터셋 생성
	 */
	@Test
	public void testBigQuery_create_dataset() {
		
		//fail("Not yet implemented");
		// Instantiate a client. If you don't specify credentials when constructing a client, the
	    // client library will look for credentials in the environment, such as the
	    // GOOGLE_APPLICATION_CREDENTIALS environment variable.
		BigQuery bigquery = BigQueryOptions.getDefaultInstance().getService();
		
		String datasetName = "DataSetGood2"; //lab
		
		Dataset dataset = null;
		DatasetInfo datasetInfo = DatasetInfo.newBuilder(datasetName).build();
		dataset = bigquery.create(datasetInfo);
		System.out.println("Dataset created : " + dataset.getDatasetId().getDataset());
	}
	
	/**
	 * 테이블 생성
	 */
	@Test
	public void testBigQuery_create_table() {
		BigQuery bigquery = BigQueryOptions.getDefaultInstance().getService();
		System.out.println("bigquery" + bigquery.toString());
		
		TableId tableId = TableId.of(datasetId, "test_table");
		// Table field definition
		Field stringField = Field.of("StringField", LegacySQLTypeName.STRING);
		// Table schema definition
		Schema schema = Schema.of(stringField);
		Field integerField = Field.of("IntegerField", LegacySQLTypeName.STRING);
		// Table schema definition
		Schema integerSchema2 = Schema.of(integerField);
		
		// Create a table
		StandardTableDefinition tableDefinition = StandardTableDefinition.of(schema);
		Table createdTable = bigquery.create(TableInfo.of(tableId, tableDefinition));
		
		System.out.println("createdTable.getTableId() : " + createdTable.getTableId());

	}
}
