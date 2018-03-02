package kr.co.bq.test;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.OutputStream;
import java.nio.channels.Channels;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;

import org.junit.Test;

import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQueryOptions;
import com.google.cloud.bigquery.Dataset;
import com.google.cloud.bigquery.DatasetInfo;
import com.google.cloud.bigquery.Field;
import com.google.cloud.bigquery.FormatOptions;
import com.google.cloud.bigquery.Job;
import com.google.cloud.bigquery.JobStatistics.LoadStatistics;
import com.google.cloud.bigquery.LegacySQLTypeName;
import com.google.cloud.bigquery.Schema;
import com.google.cloud.bigquery.StandardTableDefinition;
import com.google.cloud.bigquery.Table;
import com.google.cloud.bigquery.TableDataWriteChannel;
import com.google.cloud.bigquery.TableId;
import com.google.cloud.bigquery.TableInfo;
import com.google.cloud.bigquery.WriteChannelConfiguration;

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
		
		String datasetName = "DataSetGood3"; //lab
		
		Dataset dataset = null;
		DatasetInfo datasetInfo = DatasetInfo.newBuilder(datasetName).build();
		dataset = bigquery.create(datasetInfo);
		System.out.println("Dataset created : " + dataset.getDatasetId().getDataset());
		assertThat(dataset.getDatasetId().getDataset()).isEqualTo(datasetName);
		
	}
	
	/**
	 * 테이블 생성
	 */
	@Test
	public void testBigQuery_create_table() {
		BigQuery bigquery = BigQueryOptions.getDefaultInstance().getService();
		
		TableId tableId = TableId.of(datasetId, "lab_customers");
		// Table field definition
		List<Field> fields = new ArrayList<>();
		fields.add(Field.of("customer_id", LegacySQLTypeName.STRING));
		fields.add(Field.of("first_name", LegacySQLTypeName.STRING));
		fields.add(Field.of("last_name", LegacySQLTypeName.STRING));
		fields.add(Field.of("curent_credit_card", LegacySQLTypeName.STRING));
		fields.add(Field.of("email", LegacySQLTypeName.STRING));
		fields.add(Field.of("address_street_number", LegacySQLTypeName.STRING));
		fields.add(Field.of("address_city", LegacySQLTypeName.STRING));
		fields.add(Field.of("address_state", LegacySQLTypeName.STRING));
		fields.add(Field.of("address_zip", LegacySQLTypeName.STRING));
		fields.add(Field.of("region", LegacySQLTypeName.STRING));
		fields.add(Field.of("phone_number", LegacySQLTypeName.STRING));
		fields.add(Field.of("start_date", LegacySQLTypeName.DATE));
		fields.add(Field.of("end_date", LegacySQLTypeName.DATE));
		//Table schema definition
		Schema schema = Schema.of(fields);
		
		// Table schema definition
		
		// Create a table
		StandardTableDefinition tableDefinition = StandardTableDefinition.of(schema);
		Table createdTable = bigquery.create(TableInfo.of(tableId, tableDefinition));
		
		System.out.println("createdTable.getTableId() : " + createdTable.getTableId());
	}
	
	/**
	 * 테이블에 데이터 로드
	 * csv 파일에 헤더가 없어야 함.
	 * 날짜 형식은 YYYY-MM-DD  이여야 함. YYYY/MM/DD는  오류 발생.
	 */
	@Test
	public void testBigQuery_dataload() {
		TableDataWriteChannel writer = null;
		try {
			Path csvPath = Paths.get("C:/project/git/welcomeBQ/welcomBQ/sampledata/lab_customers.csv");
			
			BigQuery bigquery = BigQueryOptions.getDefaultInstance().getService();
			
			TableId tableId = TableId.of(datasetId, "lab_customers");
			WriteChannelConfiguration writeChannelConfiguration = WriteChannelConfiguration.newBuilder(tableId).setFormatOptions(FormatOptions.csv()).build();
			writer = bigquery.writer(writeChannelConfiguration);
			// Write data to writer
			try (OutputStream stream = Channels.newOutputStream(writer)) {
				Files.copy(csvPath, stream);
			}
			
			// Get load job
			Job job = writer.getJob();
			job = job.waitFor();
			LoadStatistics stats = job.getStatistics();
			System.out.println("stats.getOutputRows() : " + stats.getOutputRows());
		}catch(Exception e) {
			e.printStackTrace();
		}finally {
			if(writer != null) try{writer.close();}catch(Exception e) {}
		}
		
	}
}
