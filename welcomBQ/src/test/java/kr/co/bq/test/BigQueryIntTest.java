package kr.co.bq.test;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.FileWriter;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.channels.Channels;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
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
		
		String datasetName = "DataSetGood4"; //lab
		
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
		assertThat(createdTable.getTableId()).isEqualTo("lab_customers");
	}
	
	/**
	 * 테이블에 데이터 로드
	 * csv 파일에 헤더가 없어야 함.
	 * 날짜 형식은 YYYY-MM-DD 이여야 함. 날짜형식이 다르면 오류 발생.
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
			
			assertThat(stats.getOutputRows()).isEqualTo(10000);
		}catch(Exception e) {
			e.printStackTrace();
		}finally {
			if(writer != null) try{writer.close();}catch(Exception e) {}
		}
	}
	
	
	/**
	 * Dec 22 2015 같은 날짜 형식을 인식하지 못함.
	 * @param args
	 */
	public static void main(String[] args) {
		try {
			BigQueryIntTest test = new BigQueryIntTest();
			test.csvFileDateConvert();
		}catch(Exception e) {
			e.printStackTrace();
		}
	}
	
	private void csvFileDateConvert() {
		//SimpleDateFormat inSDF = new SimpleDateFormat("MMM dd yyyy");
		//SimpleDateFormat outSDF = new SimpleDateFormat("yyyy-mm-dd");
		FileWriter writer = null;
		try(Stream<String> lines = Files.lines((Paths.get("C:/project/git/welcomeBQ/welcomBQ/sampledata/lab_customers_ori.csv")))){
			List<String> values = lines.collect(Collectors.toList());
			
			Map<String,String> monthMap = getMonthMap();
			
			writer = new FileWriter("C:/project/git/welcomeBQ/welcomBQ/sampledata/lab_customers.csv");
			
			for(String value : values) {
				//System.out.println(value);
				String line = value;
				List<String> fieldList = Arrays.asList(line.split(","));
				
				for(int i = 0 ; i < fieldList.size() ; i++) {
					if(i == 11 || i == 12) {
						if(fieldList.get(i) != null &&  fieldList.get(i).length() > 0) {
							System.out.println("line.get(i) : " + fieldList.get(i));
							try{
								String inDate = fieldList.get(i);
								String[] dateArr =  inDate.split(" ");
								if(monthMap.get(dateArr[0]) != null) {
									fieldList.set(i, fieldList.get(i).replaceAll(inDate, dateArr[2]+"-"+monthMap.get(dateArr[0])+"-"+dateArr[1]));
									System.out.println("outDate : " + dateArr[2]+"-"+monthMap.get(dateArr[0])+"-"+dateArr[1]);
								}
								//Date date = inSDF.parse(inDate);
								//String outDate = outSDF.format(date);
								//System.out.println("outDate : " + outDate);
							}catch(Exception e) {
								e.printStackTrace();
							}
						}
					}
				}				
				System.out.println("line : " + line);
				System.out.println("fieldList : " + fieldList);
				
				try {
					appendLine(writer, fieldList);
				} catch (Exception e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
		}catch(IOException ie) {
			ie.printStackTrace();
		}catch(Exception e) {
			e.printStackTrace();
		}finally {
			if(writer != null) {try{writer.close();}catch(Exception e) {}}
		}
	}
	
	private Map<String, String> getMonthMap(){
		Map<String,String> monthMap = new HashMap<>();
		monthMap.put("Jan", "01");
		monthMap.put("Feb", "02");
		monthMap.put("Mar", "03");
		monthMap.put("Apr", "04");
		monthMap.put("May", "05");
		monthMap.put("Jun", "06");
		monthMap.put("Jul", "07");
		monthMap.put("Aug", "08");
		monthMap.put("Sep", "09");
		monthMap.put("Oct", "10");
		monthMap.put("Nov", "11");
		monthMap.put("Dec", "12");
		
		return monthMap;
	}
	
	private void appendLine(FileWriter writer, List<String> line) throws Exception {
		String comma = "";
		if(line.size() != 13) {
			for(int i = 0 ; i < 13-line.size() ; i++) {
				comma+= ",";
			}
		}
		writer.append(String.join(",", line)+comma+"\n");
	}
}
