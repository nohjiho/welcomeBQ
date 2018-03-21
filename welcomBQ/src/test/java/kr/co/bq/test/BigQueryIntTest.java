package kr.co.bq.test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.LocalDate;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.UUID;

import org.apache.beam.sdk.repackaged.org.apache.commons.lang3.StringUtils;
import org.junit.Ignore;
import org.junit.Test;

import com.google.api.client.util.Charsets;
import com.google.api.client.util.Data;
import com.google.api.gax.paging.Page;
import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQuery.DatasetDeleteOption;
import com.google.cloud.bigquery.BigQuery.TableDataListOption;
import com.google.cloud.bigquery.BigQuery.TableListOption;
import com.google.cloud.bigquery.BigQueryError;
import com.google.cloud.bigquery.BigQueryOptions;
import com.google.cloud.bigquery.Dataset;
import com.google.cloud.bigquery.DatasetInfo;
import com.google.cloud.bigquery.Field;
import com.google.cloud.bigquery.Field.Mode;
import com.google.cloud.bigquery.FieldValueList;
import com.google.cloud.bigquery.FormatOptions;
import com.google.cloud.bigquery.InsertAllRequest;
import com.google.cloud.bigquery.InsertAllRequest.RowToInsert;
import com.google.cloud.bigquery.InsertAllResponse;
import com.google.cloud.bigquery.Job;
import com.google.cloud.bigquery.JobId;
import com.google.cloud.bigquery.JobInfo;
import com.google.cloud.bigquery.JobStatistics.LoadStatistics;
import com.google.cloud.bigquery.LegacySQLTypeName;
import com.google.cloud.bigquery.QueryJobConfiguration;
import com.google.cloud.bigquery.QueryParameterValue;
import com.google.cloud.bigquery.QueryResponse;
import com.google.cloud.bigquery.Schema;
import com.google.cloud.bigquery.StandardTableDefinition;
import com.google.cloud.bigquery.Table;
import com.google.cloud.bigquery.TableDataWriteChannel;
import com.google.cloud.bigquery.TableId;
import com.google.cloud.bigquery.TableInfo;
import com.google.cloud.bigquery.TimePartitioning;
import com.google.cloud.bigquery.TimePartitioning.Type;
import com.google.cloud.bigquery.ViewDefinition;
import com.google.cloud.bigquery.WriteChannelConfiguration;

/**
 * BigQuery API Client test case
 * @author bestpractice80
 *
 */
public class BigQueryIntTest {
	private String datasetId = "lab";
	private String viewDatasetId = "lab_view";

	@Test
	public void test() {
		//fail("Not yet implemented");
		boolean flag = true;
		assertThat(flag).isTrue();
		System.out.println("Hello Junit!!");
	}
	
	@Test
	public void test_BigQuery() {
		//fail("Not yet implemented");
		boolean flag = true;
		BigQuery bigquery = BigQueryOptions.getDefaultInstance().getService();
		System.out.println("getHost : " + bigquery.getOptions().getHost());	//https://www.googleapis.com
		System.out.println("getLibraryVersion : " + bigquery.getOptions().getLibraryVersion());
		System.out.println("getApplicationName : " + bigquery.getOptions().getApplicationName());
		System.out.println("getProjectId : " + bigquery.getOptions().getProjectId());
		System.out.println("getUserAgent : " + bigquery.getOptions().getUserAgent());
		System.out.println("getNoRetrySettings : " + bigquery.getOptions().getNoRetrySettings());
		System.out.println("getDefaultHttpTransportOptions : " + bigquery.getOptions().getDefaultHttpTransportOptions());
		System.out.println("getRpc : " + bigquery.getOptions().getRpc());
		System.out.println("getScopedCredentials : " + bigquery.getOptions().getScopedCredentials());
		System.out.println("CREDENTIAL_ENV_NAME : " + bigquery.getOptions().CREDENTIAL_ENV_NAME);
		
		assertThat(flag).isTrue();
		System.out.println("Hello BigQuery !!");
	}
	
	/**
	 * 데이터셋 생성
	 */
	//@Ignore
	@Test
	public void testBigQuery_create_dataset() {
		// Instantiate a client. If you don't specify credentials when constructing a client, the
	    // client library will look for credentials in the environment, such as the
	    // GOOGLE_APPLICATION_CREDENTIALS environment variable.
		BigQuery bigquery = BigQueryOptions.getDefaultInstance().getService();
		String datasetName = "DataSetGood";
		
		DatasetInfo datasetInfo = DatasetInfo.newBuilder(datasetName).setDescription("테스트 데이터셋").build();
		Dataset dataset = bigquery.create(datasetInfo);
		System.out.println("Dataset created : " + dataset.getDatasetId().getDataset());
		assertThat(dataset.getDatasetId().getDataset()).isEqualTo(datasetName);
	}
	
	/**
	 * 데이터셋 조회
	 */
	//@Ignore
	@Test
	public void testBigQuery_listDatasets() {
		BigQuery bigquery = BigQueryOptions.getDefaultInstance().getService();
		
		for(Dataset dataset : bigquery.listDatasets().iterateAll()) {
			System.out.println("dataset : " + dataset.getDatasetId().getDataset());
		}
		assertThat(bigquery.listDatasets().getValues()).isNotNull();
	}
	
	/**
	 * 데이터셋 정보 변경
	 */
	//@Ignore
	@Test
	public void testBigQuery_update_dataset() {
		BigQuery bigquery = BigQueryOptions.getDefaultInstance().getService();
		
		Dataset oldDataset = bigquery.getDataset("DataSetGood");  
	    DatasetInfo datasetInfo = oldDataset.toBuilder()
	    		.setDescription("테스트 데이터셋 modified")
	    		.setFriendlyName("njhDataset").build();
	    Dataset newDataset = bigquery.update(datasetInfo);
	    
	    System.out.println("newDataset : " + newDataset.getDatasetId().getDataset());
	    System.out.println("Description : " + newDataset.getDescription());
	    System.out.println("FriendlyName : " + newDataset.getFriendlyName());
	    assertThat(newDataset.getDatasetId().getDataset()).isEqualTo("DataSetGood");
	    assertThat(newDataset.getDescription()).isEqualTo("테스트 데이터셋 modified");
	    assertThat(newDataset.getFriendlyName()).isEqualTo("njhDataset");
	}
	
	/**
	 * 데이터셋 delete
	 */
	//@Ignore
	@Test
	public void testBigQuery_delete_dataset() {
		String deleteDataset = "DataSetGood";
		BigQuery bigquery = BigQueryOptions.getDefaultInstance().getService();
		
		// [START deleteDataset]
	    Boolean deleted = bigquery.delete(deleteDataset, DatasetDeleteOption.deleteContents());
	    if (!deleted) {
	    	// the dataset was deleted
	    	System.out.println("dataset was not found");
	    	fail("dataset was not found");
	    }
	    assertThat(deleted).isTrue();
	    // [END deleteDataset]
	}
	
	/**
	 * table list select
	 */
	//@Ignore
	@Test
	public void testBigQuery_listTables() {
		BigQuery bigquery = BigQueryOptions.getDefaultInstance().getService();
		
		Page<Table> tables = bigquery.listTables(datasetId, TableListOption.pageSize(100));
		for (Table table : tables.iterateAll()) {
			// do something with the table
			System.out.println("table : " + table);
			System.out.println("table.getTableId().getTable() : " + table.getTableId().getTable());
		}
	}
	
	/**
	 * 테이블 생성
	 */
	//@Ignore
	@Test
	public void testBigQuery_create_table() {
		BigQuery bigquery = BigQueryOptions.getDefaultInstance().getService();
		
		TableId tableId = TableId.of(datasetId, "lab_customers");
		// Table field definition
		List<Field> fields = new ArrayList<>();
		fields.add(Field.of("customer_id", LegacySQLTypeName.STRING).toBuilder().setDescription("고객번호").setMode(Mode.REQUIRED).build());
		fields.add(Field.of("first_name", LegacySQLTypeName.STRING).toBuilder().setDescription("첫번째 이름").setMode(Mode.REQUIRED).build());
		fields.add(Field.of("last_name", LegacySQLTypeName.STRING).toBuilder().setDescription("마지막 이름").setMode(Mode.REQUIRED).build());
		fields.add(Field.of("curent_credit_card", LegacySQLTypeName.STRING).toBuilder().setDescription("카드번호").setMode(Mode.NULLABLE).build());
		fields.add(Field.of("email", LegacySQLTypeName.STRING).toBuilder().setDescription("이메일").setMode(Mode.NULLABLE).build());
		fields.add(Field.of("address_street_number", LegacySQLTypeName.STRING).toBuilder().setDescription("거리번호").setMode(Mode.NULLABLE).build());
		fields.add(Field.of("address_city", LegacySQLTypeName.STRING).toBuilder().setDescription("도시 주소").setMode(Mode.NULLABLE).build());
		fields.add(Field.of("address_state", LegacySQLTypeName.STRING).toBuilder().setDescription("국가 주소 ").setMode(Mode.NULLABLE).build());
		fields.add(Field.of("address_zip", LegacySQLTypeName.STRING).toBuilder().setDescription("우편번호").setMode(Mode.NULLABLE).build());
		fields.add(Field.of("region", LegacySQLTypeName.STRING).toBuilder().setDescription("지역").setMode(Mode.NULLABLE).build());
		fields.add(Field.of("phone_number", LegacySQLTypeName.STRING).toBuilder().setDescription("전화번호").setMode(Mode.NULLABLE).build());
		fields.add(Field.of("start_date", LegacySQLTypeName.DATE).toBuilder().setDescription("시작일자").setMode(Mode.NULLABLE).build());
		fields.add(Field.of("end_date", LegacySQLTypeName.DATE).toBuilder().setDescription("종료일자").setMode(Mode.NULLABLE).build());
		//Table schema definition
		Schema schema = Schema.of(fields);
		
		// Create a table
		StandardTableDefinition tableDefinition = StandardTableDefinition.of(schema);
		Table createdTable = bigquery.create(TableInfo.of(tableId, tableDefinition).toBuilder()
				.setDescription("고객 정보").build());
		
		System.out.println("createdTable.getTableId() : " + createdTable.getTableId());
		assertThat(createdTable.getTableId().getTable()).isEqualTo("lab_customers");
	}
	
	/**
	 * 일별 파티셔닝 테이블 생성
	 */
	//@Ignore
	@Test
	public void testBigQuery_create_table_patitioning() {
		BigQuery bigquery = BigQueryOptions.getDefaultInstance().getService();
		
		TableId tableId = TableId.of(datasetId, "lab_customers");
		// Table field definition
		List<Field> fields = new ArrayList<>();
		fields.add(Field.of("customer_id", LegacySQLTypeName.STRING).toBuilder().setDescription("고객번호").setMode(Mode.REQUIRED).build());
		fields.add(Field.of("first_name", LegacySQLTypeName.STRING).toBuilder().setDescription("첫번째 이름").setMode(Mode.REQUIRED).build());
		fields.add(Field.of("last_name", LegacySQLTypeName.STRING).toBuilder().setDescription("마지막 이름").setMode(Mode.REQUIRED).build());
		fields.add(Field.of("curent_credit_card", LegacySQLTypeName.STRING).toBuilder().setDescription("카드번호").setMode(Mode.NULLABLE).build());
		fields.add(Field.of("email", LegacySQLTypeName.STRING).toBuilder().setDescription("이메일").setMode(Mode.NULLABLE).build());
		fields.add(Field.of("address_street_number", LegacySQLTypeName.STRING).toBuilder().setDescription("거리번호").setMode(Mode.NULLABLE).build());
		fields.add(Field.of("address_city", LegacySQLTypeName.STRING).toBuilder().setDescription("도시 주소").setMode(Mode.NULLABLE).build());
		fields.add(Field.of("address_state", LegacySQLTypeName.STRING).toBuilder().setDescription("국가 주소 ").setMode(Mode.NULLABLE).build());
		fields.add(Field.of("address_zip", LegacySQLTypeName.STRING).toBuilder().setDescription("우편번호").setMode(Mode.NULLABLE).build());
		fields.add(Field.of("region", LegacySQLTypeName.STRING).toBuilder().setDescription("지역").setMode(Mode.NULLABLE).build());
		fields.add(Field.of("phone_number", LegacySQLTypeName.STRING).toBuilder().setDescription("전화번호").setMode(Mode.NULLABLE).build());
		fields.add(Field.of("start_date", LegacySQLTypeName.DATE).toBuilder().setDescription("시작일자").setMode(Mode.NULLABLE).build());
		fields.add(Field.of("end_date", LegacySQLTypeName.DATE).toBuilder().setDescription("종료일자").setMode(Mode.NULLABLE).build());
		//Table schema definition
		Schema schema = Schema.of(fields);
		
		// Create a daily partitioning table
		StandardTableDefinition tableDefinition = StandardTableDefinition.newBuilder()
		        .setSchema(schema)
		        .setTimePartitioning(TimePartitioning.of(Type.DAY))
		        .build();
		Table createdTable = bigquery.create(TableInfo.of(tableId, tableDefinition).toBuilder()
				.setDescription("고객 정보").build());
		
		System.out.println("createdTable.getTableId() : " + createdTable.getTableId());
		assertThat(createdTable.getTableId().getTable()).isEqualTo("lab_customers");
	}
	
	/**
	 * lab_transactions 테이블 생성
	 */
	@Ignore
	@Test
	public void testBigQuery_create_table_trans() {
		BigQuery bigquery = BigQueryOptions.getDefaultInstance().getService();
		
		TableId tableId = TableId.of(datasetId, "lab_transactions");
		// Table field definition
		List<Field> fields = new ArrayList<>();
		fields.add(Field.of("customer_id", LegacySQLTypeName.STRING).toBuilder().setDescription("고객번호").setMode(Mode.REQUIRED).build());
		fields.add(Field.of("transaction_date", LegacySQLTypeName.DATE).toBuilder().setDescription("업무일자").setMode(Mode.REQUIRED).build());
		fields.add(Field.of("ticket_price", LegacySQLTypeName.FLOAT).toBuilder().setDescription("티켓 가격").setMode(Mode.REQUIRED).build());
		fields.add(Field.of("discount", LegacySQLTypeName.STRING).toBuilder().setDescription("할인률").setMode(Mode.NULLABLE).build());
		fields.add(Field.of("product", LegacySQLTypeName.STRING).toBuilder().setDescription("제품").setMode(Mode.REQUIRED).build());
		//Table schema definition
		Schema schema = Schema.of(fields);
		
		// Create a table
		StandardTableDefinition tableDefinition = StandardTableDefinition.of(schema);
		Table createdTable = bigquery.create(TableInfo.of(tableId, tableDefinition).toBuilder()
				.setDescription("테스트용 고객 구매내역 정보").build());
		
		System.out.println("createdTable.getTableId() : " + createdTable.getTableId());
		assertThat(createdTable.getTableId().getTable()).isEqualTo("lab_transactions");
	}
	
	/**
	 * 일별 파티셔닝 테이블 생성
	 */
	//@Ignore
	@Test
	public void testBigQuery_create_table_trans_patitioning() {
		BigQuery bigquery = BigQueryOptions.getDefaultInstance().getService();
		
		TableId tableId = TableId.of(datasetId, "lab_transactions");
		// Table field definition
		List<Field> fields = new ArrayList<>();
		fields.add(Field.of("customer_id", LegacySQLTypeName.STRING).toBuilder().setDescription("고객번호").setMode(Mode.REQUIRED).build());
		fields.add(Field.of("transaction_date", LegacySQLTypeName.DATE).toBuilder().setDescription("업무일자").setMode(Mode.REQUIRED).build());
		fields.add(Field.of("ticket_price", LegacySQLTypeName.FLOAT).toBuilder().setDescription("티켓 가격").setMode(Mode.REQUIRED).build());
		fields.add(Field.of("discount", LegacySQLTypeName.STRING).toBuilder().setDescription("할인률").setMode(Mode.NULLABLE).build());
		fields.add(Field.of("product", LegacySQLTypeName.STRING).toBuilder().setDescription("제품").setMode(Mode.REQUIRED).build());
		//Table schema definition
		Schema schema = Schema.of(fields);
		
		// Create a daily partitioning table
		TimePartitioning partitioning = TimePartitioning.of(Type.DAY);
		StandardTableDefinition tableDefinition = StandardTableDefinition.newBuilder()
		        .setSchema(schema)
		        .setTimePartitioning(partitioning)
		        .build();
		Table createdTable = bigquery.create(TableInfo.of(tableId, tableDefinition).toBuilder()
				.setDescription("테스트용 고객 구매내역 정보").build());
		
		System.out.println("createdTable.getTableId() : " + createdTable.getTableId());
		assertThat(createdTable.getTableId().getTable()).isEqualTo("lab_transactions");
	}
	
	/**
	 * 테이블에 데이터 로드
	 * 파일 전체 데이터 중 하나의 row라도 제약조건에 맞지 않으면 전체가 등록되지 않음.
	 * csv 파일에 헤더가 없어야 함.
	 * 날짜 형식은 YYYY-MM-DD 이여야 함. 날짜형식이 다르면 오류 발생.
	 * partitioning : tableId$20180306 (partitionDate)
	 */
	//@Ignore
	@Test
	public void testBigQuery_writeCsvFileToTable() {
		TableDataWriteChannel writer = null;
		try {
			Path csvPath = Paths.get("C:/project/git/welcomeBQ/welcomBQ/sampledata/lab_customers.csv");
			
			BigQuery bigquery = BigQueryOptions.getDefaultInstance().getService();
			//테이블명$파티션명 
			TableId tableId = TableId.of(datasetId, "lab_customers$20180308");
			WriteChannelConfiguration writeChannelConfiguration = WriteChannelConfiguration.newBuilder(tableId).setFormatOptions(FormatOptions.csv()).build();
			writer = bigquery.writer(writeChannelConfiguration);
			// Write data to writer
			try(OutputStream stream = Channels.newOutputStream(writer)) {
				Files.copy(csvPath, stream);
			}
			
			// Get load job
			Job job = writer.getJob();
			job = job.waitFor();
			LoadStatistics stats = job.getStatistics();
			System.out.println("stats.getOutputRows() : " + stats.getOutputRows());
			
			assertThat(stats.getOutputRows()).isEqualTo(10000);
		}catch(IOException e) {
			e.printStackTrace();
		}catch(InterruptedException e) {
			e.printStackTrace();
		}catch(Exception e){
			e.printStackTrace();;
		}finally {
			if(writer != null) try{writer.close();}catch(Exception e) {}
		}
	}
	
	/**
	 * 테이블에 데이터 로드(lab_2013_transactions.csv , lab_2014_transactions.csv , lab_2015_transactions.csv)
	 * 4MB --> 39s 
	 * 11M -> 71s
	 */
	//@Ignore
	@Test
	public void testBigQuery_writeCsvFileToTable_trans() {
		TableDataWriteChannel writer = null;
		try {
			Path csvPath = Paths.get("C:/project/git/welcomeBQ/welcomBQ/sampledata/lab_2017_transactions.csv");
			
			BigQuery bigquery = BigQueryOptions.getDefaultInstance().getService();
			TableId tableId = TableId.of(datasetId, "lab_transactions$20170101");
			WriteChannelConfiguration writeChannelConfiguration = WriteChannelConfiguration.newBuilder(tableId).setFormatOptions(FormatOptions.csv()).build();
			writer = bigquery.writer(writeChannelConfiguration);
			// Write data to writer
			try(OutputStream stream = Channels.newOutputStream(writer)) {
				Files.copy(csvPath, stream);
			}
			
			// Get load job
			Job job = writer.getJob();
			job = job.waitFor();
			LoadStatistics stats = job.getStatistics();
			System.out.println("stats.getOutputRows() : " + stats.getOutputRows());
			
			//assertThat(stats.getOutputRows()).isEqualTo(60326);
			assertThat(stats.getOutputRows()).isNotEqualTo(0);
		}catch(IOException e) {
			e.printStackTrace();
		}catch(InterruptedException e) {
			e.printStackTrace();
		}catch(Exception e){
			e.printStackTrace();
		}finally {
			if(writer != null) try{writer.close();}catch(Exception e) {}
		}
	}
	
	/**
	 * 단건 등록
	 */
	//@Ignore
	@Test
	public void testBigQuery_writeToTable() {
		String csvData = "11111111111111111111111111111111,ERINN,DZVONIK,4189090828557714,bestpractice80@gmail.com,9195 George Street,Cleveland,OH,44121,midwest,(216)926-9604,2018-03-09,2040-12-01";
		try {
			
			// [START updateTable]
			BigQuery bigquery = BigQueryOptions.getDefaultInstance().getService();
			// [START writeToTable]
		    TableId tableId = TableId.of(datasetId, "lab_customers$20180312");
		    WriteChannelConfiguration writeChannelConfiguration = WriteChannelConfiguration.newBuilder(tableId)
		    		.setFormatOptions(FormatOptions.csv())
		    		.build();
		    TableDataWriteChannel writer = bigquery.writer(writeChannelConfiguration);
		    // Write data to writer
		    try {
		    	writer.write(ByteBuffer.wrap(csvData.getBytes(Charsets.UTF_8)));
		    }finally {
		    	writer.close();
		    }
		    // Get load job
		    Job job = writer.getJob();
		    job = job.waitFor();
		    LoadStatistics stats = job.getStatistics();
		    
		    System.out.println("stats.getOutputRows() " + stats.getOutputRows());
		    assertThat(stats.getOutputRows()).isEqualTo(1);
		    // [END writeToTable]
		}catch(IOException e) {
			e.printStackTrace();
		}catch(InterruptedException e) {
			e.printStackTrace();
		}catch(Exception e){
			e.printStackTrace();
		}
	}
	
	/**
	 * streaming insert into 빅쿼리
	 * UPDATE or DELETE DML statements are not supported over tables with streaming buffer
	 * DML문 Streaming buffer가 있는 테이블에 지원되지 않음. -  꼭 필요한 경우만 사용하는게 좋을 듯.
	 * 피티션값이 7일전,3일후 까지 또는 null일 경우 스트리밍 데이터가 허용됩니다. 
	 */
	//@Ignore
	@Test
	public void testBigQuery_insertAll() {
		try {
			BigQuery bigquery = BigQueryOptions.getDefaultInstance().getService();
			// [START insertAll]
		    TableId tableId = TableId.of(datasetId, "lab_customers$20180312");
		    List<RowToInsert> rowContents = new ArrayList<>();
		    // Values of the row to insert
		    Map<String, Object> rowContent = null;
		    for(int i = 0 ; i < 10000 ; i++) {
		    	String rowId = ZonedDateTime.now().format(DateTimeFormatter.ofPattern("yyyyMMddHHmmssSSSSSS"))+i;
		    	String customerId = StringUtils.leftPad(rowId, 28, "0");
		    	
		    	System.out.println("customerId : " + customerId);
		    	System.out.println("rowId : " + rowId);
				rowContent = new HashMap<>();
				rowContent.put("customer_id", customerId);
				rowContent.put("first_name", "Noh");
				rowContent.put("last_name", "Jiho");
				rowContent.put("curent_credit_card", "1111111111111");
				rowContent.put("email", "bigquerytest80@gmail.com");
				rowContent.put("address_street_number", "");
				rowContent.put("address_city", "seoul");
				rowContent.put("address_state", "George Street");
				rowContent.put("address_zip", "44121");
				rowContent.put("region", "");
				rowContent.put("phone_number", "010-9999-9999");
				rowContent.put("start_date", "2018-03-09");
				rowContent.put("end_date", "2040-01-01");
				//id는중복된 행을 식별하는데 사용됨.
				rowContents.add(RowToInsert.of(rowId, rowContent));	
		    }
		    
		    InsertAllResponse response = bigquery.insertAll(InsertAllRequest.newBuilder(tableId)
		    		//rowId를 지정하면, 해당 row에 등록 동일한 rowId가 있으면 update
		    		//.addRow(UUID.randomUUID().toString(), rowContent)
		    		// More rows can be added in the same RPC by invoking .addRow() on the builder
		    		.setRows(rowContents)
		    		.build());
		    if (response.hasErrors()) {
		    	// If any of the insertions failed, this lets you inspect the errors
		    	for (Entry<Long, List<BigQueryError>> entry : response.getInsertErrors().entrySet()) {
		    		// inspect row error
		    		System.out.println("error entry : " + entry);
		    	}
		    	fail("error");
		    }
		    // [END insertAll]
		    System.out.println("response : " + response);
		    assertThat(response.hasErrors()).isFalse();
		    // [END writeToTable]
		}catch(Exception e){
			e.printStackTrace();
		}
	}
	
	/**
	 * 테이블 뷰 생성
	 * 테이블 뷰는 Standard SQL을 사용할 수 없으므로 , Lagacy SQL을 사용해야 한다.
	 */
	//@Ignore
	@Test
	public void testBigQuery_create_viewtable() {
		BigQuery bigquery = BigQueryOptions.getDefaultInstance().getService();
		
		TableId tableId = TableId.of(viewDatasetId, "v_lab_customers");	//viewDataSetId : lab_view
		
		String query = ""
				+ " select "
				+ " 	_PARTITIONDATE as pt , "
				+ " 	customer_id , "
				+ " 	first_name , "
				+ " 	last_name , "
				+ " 	curent_credit_card , "
				+ " 	email , address_city , "
				+ " 	concat(address_street_number , ' ' , address_state , ' ' ,  address_zip , ' ' ,  region) as addr , "
				+ " 	phone_number , "
				+ " 	start_date , "
				+ " 	end_date  "
				+ " from lab.lab_customers ";
		Table createdTable = bigquery.create(TableInfo.of(tableId, ViewDefinition.of(query)).toBuilder()
				.setDescription("고객 정보 뷰").build());
		
		System.out.println("createdTable.getTableId() : " + createdTable.getTableId());
		assertThat(createdTable.getTableId().getTable()).isEqualTo("v_lab_customers");
	}
	
	
	/**
	 * table info modify
	 */
	//@Ignore
	@Test
	public void testBigQuery_updateTable() {
		// [START updateTable]
		BigQuery bigquery = BigQueryOptions.getDefaultInstance().getService();
	    Table oldTable = bigquery.getTable(datasetId, "lab_customers");
	    System.out.println("oldTable : " + oldTable);

	    TableInfo tableInfo = oldTable.toBuilder()
	    		.setDescription("test table")
	    		.setFriendlyName("njh_customers").build();
	    Table newTable = bigquery.update(tableInfo);
	    // [END updateTable]
	    System.out.println("newTable.getFriendlyName() : " + newTable.getFriendlyName());
	    assertThat(newTable.getFriendlyName()).isEqualTo("njh_customers");
	}
	
	/**
	 * delete partition
	 * streaming insert 데이터는 등록하자마자 바로 삭제 되지 않는다.
	 * 일정 시간이 지나야 삭제됨. 최대 90분정도?
	 */
	//@Ignore
	@Test
	public void testBigQuery_delete_partition() {
		BigQuery bigquery = BigQueryOptions.getDefaultInstance().getService();
		// [START delete partition]
	    Boolean deleted = bigquery.delete(datasetId, "lab_customers$20180312");
	    // [END delete partition]
	    if (!deleted) {
	    	// the table was not found
	    	fail("the table was not found");
	    }
	    
	    System.out.println("deleted partition!!");
	    assertThat(deleted).isTrue();
	}
	
	/**
	 * drop table
	 */
	//@Ignore
	@Test
	public void testBigQuery_delete_table() {
		BigQuery bigquery = BigQueryOptions.getDefaultInstance().getService();
		// [START deleteTable]
	    Boolean deleted = bigquery.delete(viewDatasetId, "v_lab_customers");
	    if (!deleted) {
	    	// the table was not found
	    	fail("the table was not found");
	    }
	    
	    System.out.println("deleted table!!");
	    assertThat(deleted).isTrue();
	}
	
	/**
	 * 페이지를 지정하여 테이블 전제 데이터 조회 
	 * RPC(Remote Procedue Call) : 프로세스간 통신 기법 중 하나로 , IDL(Interface Definition Language)을 활용해서 
	 * 네트워크 통신과 관련된 작업은 신경쓰지 않고, 원격지(Google Cloud)의 프로그램을 로컬에 있는 프로그램처럼 사용.  
	 * http2 프로토콜 사용
	 */
	//@Ignore
	@Test
	public void testBigQuery_listTableData() {
		// [START listTableData]
		BigQuery bigquery = BigQueryOptions.getDefaultInstance().getService();
	    // This example reads the result 100 rows per RPC(Remote Procedure Call) call. If there's no need to limit the number,
	    // simply omit the option.
		Page<FieldValueList> tableData =
				bigquery.listTableData(datasetId, "lab_customers", TableDataListOption.pageSize(100));
				//bigquery.listTableData(datasetId, "lab_customers");
		Iterable<FieldValueList> fieldValueLists = tableData.iterateAll();

		fieldValueLists.forEach(fieldValueList -> {
			fieldValueList.forEach(fieldValue -> {
				System.out.println("field value : " + fieldValue.getValue());
			});
			System.out.println("==========================");
		});
	    // [END listTableData]
		assertThat(tableData).isNotNull();
	}
		
	@Test
	public void testBigQuery_QueryingData() {
		// [START bigquery_simple_app_client]
	    BigQuery bigquery = BigQueryOptions.getDefaultInstance().getService();
	    // [END bigquery_simple_app_client]
	    // [START bigquery_simple_app_query]
	    QueryJobConfiguration queryConfig =
	        QueryJobConfiguration.newBuilder(
	        		"   select "
	        		+ "		customer_id , "
	        		+ " 	email , "
	        		+ "     current_date() as now_date "
	        		+ " from lab.lab_customers "
    				+ " where start_date between CAST(@fromDate AS DATE) and CAST(@toDate AS DATE) "
    				+ "   and end_date is not null "
    				+ "   and email like '%gmail%' "
    				+ "   and region in UNNEST(@region) "
    				+ " order by start_date desc "
    				+ " limit 10 ")
	            // Use standard SQL syntax for queries.
	            // See: https://cloud.google.com/bigquery/sql-reference/
	        	.addNamedParameter("fromDate", QueryParameterValue.string("2015-01-01"))
	        	.addNamedParameter("toDate", QueryParameterValue.string("2017-01-01"))
	        	.addNamedParameter("region", QueryParameterValue.array(new String[] {"south" , "midwest"},String.class))
	            .setUseLegacySql(false)	//legacy : ture , standard : false 
	            .build();

	    // Create a job ID so that we can safely retry.
	    JobId jobId = JobId.of(ZonedDateTime.now().format(DateTimeFormatter.ofPattern("yyyyMMddHHmmssSSSSSS")));
	    Job queryJob = bigquery.create(JobInfo.newBuilder(queryConfig).setJobId(jobId).build());

	    // Wait for the query to complete.
	    try {
			queryJob = queryJob.waitFor();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}

	    // Check for errors
	    if (queryJob == null) {
	    	throw new RuntimeException("Job no longer exists");
	    } else if (queryJob.getStatus().getError() != null) {
	    	// You can also look at queryJob.getStatus().getExecutionErrors() for all
	    	// errors, not just the latest one.
	    	throw new RuntimeException(queryJob.getStatus().getError().toString());
	    }
	    // [END bigquery_simple_app_query]

	    // [START bigquery_simple_app_print]
	    // Get the results.
	    QueryResponse response = bigquery.getQueryResults(jobId);
	    // Print all pages of the results.
	    for (FieldValueList row : response.getResult().iterateAll()) {
	    	String customerId = row.get("customer_id").getStringValue();
	    	String email = row.get("email").getStringValue();
	    	String nowDate = row.get("now_date").getStringValue();
	    	System.out.println("customerId : " + customerId);
	    	System.out.println("email : " + email);
	    	System.out.println("nowDate : " + nowDate);
	    }
	    
	    assertThat(response.getResult().getTotalRows()).isNotEqualTo(0);
	    // [END bigquery_simple_app_print]
	}
	
	//@Ignore
	@Test
	public void testBigQuery_join() {
		// [START bigquery_simple_app_client]
	    BigQuery bigquery = BigQueryOptions.getDefaultInstance().getService();
	    // [END bigquery_simple_app_client]
	    // [START bigquery_simple_app_query]
	    QueryJobConfiguration queryConfig =
	        QueryJobConfiguration.newBuilder(
	        		" select " + 
	        		"     t1.customer_id , " + 
	        		"     t1.email , " + 
	        		"     t2.transaction_date , " + 
	        		"     t2.ticket_price , " + 
	        		"     t2.discount , " + 
	        		"     t2.product " + 
	        		" from lab.lab_customers t1 " + 
	        		" join " +
	        		//" left join " + 
	        		" lab.lab_transactions t2 " + 
	        		" on t1.customer_id = t2.customer_id " + 
	        		" WHERE t1._PARTITIONTIME >= @fromDate AND t1._PARTITIONTIME < @toDate " + 
	        		"   and t1.email like concat('%', @email,'%') " + 
	        		"   and t2.discount is not null " + 
	        		" order by t2.transaction_date asc " + 
	        		" limit 100 ")
	            // Use standard SQL syntax for queries.
	            // See: https://cloud.google.com/bigquery/sql-reference/
	        	.addNamedParameter("fromDate", QueryParameterValue.string("2018-03-08 00:00:00"))
	        	.addNamedParameter("toDate", QueryParameterValue.string("2018-03-10 00:00:00"))
	        	.addNamedParameter("email", QueryParameterValue.string("gmail"))
	            .setUseLegacySql(false)	//legacy : ture , standard : false 
	            .build();

	    // Create a job ID so that we can safely retry.
	    JobId jobId = JobId.of(UUID.randomUUID().toString());
	    Job queryJob = bigquery.create(JobInfo.newBuilder(queryConfig).setJobId(jobId).build());

	    // Wait for the query to complete.
	    try {
			queryJob = queryJob.waitFor();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}

	    // Check for errors
	    if (queryJob == null) {
	    	throw new RuntimeException("Job no longer exists");
	    } else if (queryJob.getStatus().getError() != null) {
	    	// You can also look at queryJob.getStatus().getExecutionErrors() for all
	    	// errors, not just the latest one.
	    	throw new RuntimeException(queryJob.getStatus().getError().toString());
	    }
	    // [END bigquery_simple_app_query]

	    // [START bigquery_simple_app_print]
	    // Get the results.
	    QueryResponse response = bigquery.getQueryResults(jobId);
	    System.out.println("response : " + response);
	    // Print all pages of the results.
	    for (FieldValueList row : response.getResult().iterateAll()) {
	    	String customerId = row.get("customer_id").getStringValue();
	    	String email = row.get("email").getStringValue();
	    	String transDate = row.get("transaction_date").getStringValue();
	    	Double ticketPrice = row.get("ticket_price").getDoubleValue();
	    	String discount = row.get("discount").getStringValue();
	    	String product = row.get("product").getStringValue();
	    	
	    	System.out.println("customerId : " + customerId);
	    	System.out.println("email : " + email);
	    	System.out.println("transDate : " + transDate);
	    	System.out.println("ticketPrice : " + ticketPrice);
	    	System.out.println("discount : " + discount);
	    	System.out.println("product : " + product);
	    	System.out.println("====================");
	    }
	    
	    assertThat(response.getResult().getTotalRows()).isNotEqualTo(0);
	    // [END bigquery_simple_app_print]
	}
	
	/**
	 * Standard SQL insert 
	 * insert 시 파티션 값을 입력하지 않으면 Default로 current_date()로 지정된다.
	 */
	//@Ignore
	@Test
	public void testBigQuery_insert_row_partition_table() {
		// [START bigquery_simple_app_client]
	    BigQuery bigquery = BigQueryOptions.getDefaultInstance().getService();
	    // [END bigquery_simple_app_client]
	    // [START bigquery_simple_app_query]
	    QueryJobConfiguration queryConfig =
	        QueryJobConfiguration.newBuilder(
	        		"insert into lab.lab_customers " + 
    				"( " + 
    				"  _PARTITIONTIME , " + 
    				"  customer_id , " +
    				"  first_name , " + 
    				"  last_name , " + 
    				"  email , " + 
    				"  start_date , " + 
    				"  end_date" + 
    				") " + 
    				"values " + 
    				"( " + 
    				"  @partitionId , " + 
    				"  @customerId , " + 
    				"  @firstName , " +
    				"  @lastName , " +
    				"  @email , " + 
    				"  @startDate , " + 
    				"  @endDate " + 
    				") ")
	            // Use standard SQL syntax for queries.
	            // See: https://cloud.google.com/bigquery/sql-reference/
	        	.addNamedParameter("partitionId", QueryParameterValue.string("2018-03-13"))
	        	.addNamedParameter("customerId", QueryParameterValue.string(ZonedDateTime.now().format(DateTimeFormatter.ofPattern("yyyyMMddHHmmssSSSSSS"))))
	        	.addNamedParameter("firstName", QueryParameterValue.string("Noh"))
	        	.addNamedParameter("lastName", QueryParameterValue.string("Jiho"))
	        	.addNamedParameter("email", QueryParameterValue.string("bestpractice80@gmail.com"))
	        	.addNamedParameter("startDate", QueryParameterValue.date(LocalDate.now().toString()))
	        	.addNamedParameter("endDate", QueryParameterValue.string(LocalDate.now().withYear(LocalDate.now().getYear()+3).toString()))
	            .setUseLegacySql(false)	//legacy : ture , standard : false 
	            .build();

	    // Create a job ID so that we can safely retry.
	    JobId jobId = JobId.of(UUID.randomUUID().toString());
	    Job queryJob = bigquery.create(JobInfo.newBuilder(queryConfig).setJobId(jobId).build());

	    // Wait for the query to complete.
	    try {
			queryJob = queryJob.waitFor();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}

	    // Check for errors
	    if (queryJob == null) {
	    	throw new RuntimeException("Job no longer exists");
	    } else if (queryJob.getStatus().getError() != null) {
	    	// You can also look at queryJob.getStatus().getExecutionErrors() for all
	    	// errors, not just the latest one.
	    	throw new RuntimeException(queryJob.getStatus().getError().toString());
	    }
	    // [END bigquery_simple_app_query]

	    // [START bigquery_simple_app_print]
	    // Get the results.
	    QueryResponse response = bigquery.getQueryResults(jobId);
	    System.out.println("response : " + response);
	    
	    assertThat(response.getNumDmlAffectedRows()).isEqualTo(1);
	    // [END bigquery_simple_app_print]
	}
	
	/**
	 * dml update sql
	 * Table{tableId={datasetId=lab, projectId=hello-bigquery-196407, tableId=lab_customers}, etag=null, generatedId=hello-bigquery-196407:lab.lab_customers, selfLink=null, friendlyName=null, description=null, expirationTime=null, creationTime=null, lastModifiedTime=null, definition=StandardTableDefinition{type=TABLE, schema=null, numBytes=null, numRows=null, location=null, streamingBuffer=null, timePartitioning=null}}
	 * 데이터 등록 후 최대 90분이 있어야 수정 가능.
	 */
	//@Ignore
	@Test
	public void testBigQuery_update_row() {
		// [START bigquery_simple_app_client]
	    BigQuery bigquery = BigQueryOptions.getDefaultInstance().getService();
	    // [END bigquery_simple_app_client]
	    // [START bigquery_simple_app_query]
	    QueryJobConfiguration queryConfig =
	        QueryJobConfiguration.newBuilder(
	        		"   update lab.lab_customers "
	        		+ " set "
	        		+ "     email = @email "
    				+ " where _PARTITIONTIME >= @fromDate "
    				+ "   and _PARTITIONTIME < @toDate ")
	            // Use standard SQL syntax for queries.
	            // See: https://cloud.google.com/bigquery/sql-reference/
	        .addNamedParameter("email", QueryParameterValue.string("madvires@gmail.com"))	
	        .addNamedParameter("fromDate", QueryParameterValue.string("2018-03-13 00:00:00"))
	        	.addNamedParameter("toDate", QueryParameterValue.string("2018-03-14 00:00:00"))
	            .setUseLegacySql(false)	//legacy : ture , standard : false 
	            .build();

	    // Create a job ID so that we can safely retry.
	    JobId jobId = JobId.of(UUID.randomUUID().toString());
	    Job queryJob = bigquery.create(JobInfo.newBuilder(queryConfig).setJobId(jobId).build());

	    // Wait for the query to complete.
	    try {
			queryJob = queryJob.waitFor();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}

	    // Check for errors
	    if (queryJob == null) {
	    	throw new RuntimeException("Job no longer exists");
	    } else if (queryJob.getStatus().getError() != null) {
	    	// You can also look at queryJob.getStatus().getExecutionErrors() for all
	    	// errors, not just the latest one.
	    	throw new RuntimeException(queryJob.getStatus().getError().toString());
	    }
	    // [END bigquery_simple_app_query]

	    // [START bigquery_simple_app_print]
	    // Get the results.
	    QueryResponse response = bigquery.getQueryResults(jobId);
	    System.out.println("response : " + response);
	    System.out.println("update row count : " + response.getNumDmlAffectedRows());
	    
	    assertThat(response.getNumDmlAffectedRows()).isNotEqualTo(0);
	    // [END bigquery_simple_app_print]
	}
	
	/**
	 * dml delete sql
	 * Table{tableId={datasetId=lab, projectId=hello-bigquery-196407, tableId=lab_customers}, etag=null, generatedId=hello-bigquery-196407:lab.lab_customers, selfLink=null, friendlyName=null, description=null, expirationTime=null, creationTime=null, lastModifiedTime=null, definition=StandardTableDefinition{type=TABLE, schema=null, numBytes=null, numRows=null, location=null, streamingBuffer=null, timePartitioning=null}}
	 * 데이터 등록 후 최대 90분이 있어야 삭제 가능.
	 * BigQuery는 다중 명령문 트렌젝션을 지원하지 않음.
	 * dml 문 실행 후 auto commit
	 */
	//@Ignore
	@Test
	public void testBigQuery_delete_row() {
		// [START bigquery_simple_app_client]
	    BigQuery bigquery = BigQueryOptions.getDefaultInstance().getService();
	    // [END bigquery_simple_app_client]
	    // [START bigquery_simple_app_query]
	    QueryJobConfiguration queryConfig =
	        QueryJobConfiguration.newBuilder(
	        		"   delete from lab.lab_customers "
    				+ " where _PARTITIONTIME >= @fromDate "
    				+ "   and _PARTITIONTIME < @toDate ")
	            // Use standard SQL syntax for queries.
	            // See: https://cloud.google.com/bigquery/sql-reference/
	        	.addNamedParameter("fromDate", QueryParameterValue.string("2018-03-13 00:00:00"))
	        	.addNamedParameter("toDate", QueryParameterValue.string("2018-03-14 00:00:00"))
	            .setUseLegacySql(false)	//legacy : ture , standard : false 
	            .build();

	    // Create a job ID so that we can safely retry.
	    JobId jobId = JobId.of(UUID.randomUUID().toString());
	    Job queryJob = bigquery.create(JobInfo.newBuilder(queryConfig).setJobId(jobId).build());

	    // Wait for the query to complete.
	    try {
			queryJob = queryJob.waitFor();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}

	    // Check for errors
	    if (queryJob == null) {
	    	throw new RuntimeException("Job no longer exists");
	    } else if (queryJob.getStatus().getError() != null) {
	    	// You can also look at queryJob.getStatus().getExecutionErrors() for all
	    	// errors, not just the latest one.
	    	throw new RuntimeException(queryJob.getStatus().getError().toString());
	    }
	    // [END bigquery_simple_app_query]

	    // [START bigquery_simple_app_print]
	    // Get the results.
	    QueryResponse response = bigquery.getQueryResults(jobId);
	    System.out.println("response : " + response);
	    System.out.println("delete row count : " + response.getNumDmlAffectedRows());
	    assertThat(response.getNumDmlAffectedRows()).isNotEqualTo(0);
	    // [END bigquery_simple_app_print]
	}
	
	/**
	 * 구글 빅쿼리는 field 삭제 기능을 지원하지 않음.
	 */
	//@Ignore
	@Test
	public void testBigQuery_add_field() {
		// [START updateTable]
		BigQuery bigquery = BigQueryOptions.getDefaultInstance().getService();
	    Table oldTable = bigquery.getTable(datasetId, "lab_customers");
	    System.out.println("oldTable : " + oldTable);
	    // Table field definition
		List<Field> fields = new ArrayList<>();
		fields.addAll(oldTable.getDefinition().getSchema().getFields());
		fields.add(Field.of("join_yn", LegacySQLTypeName.STRING).toBuilder()
				.setDescription("가입여부").setMode(Mode.NULLABLE).build());
		
	    // Create a table
	    Table newTable = bigquery.update(
	    		oldTable.toBuilder().setDefinition(StandardTableDefinition.of(Schema.of(fields))).build());
	    // [END updateTable]
	    System.out.println("add Field Name : " + newTable.getDefinition().getSchema().getFields().get("join_yn").getName());
	    assertThat(newTable.getDefinition().getSchema().getFields().get("join_yn").getName()).isEqualTo("join_yn");
	}
}
