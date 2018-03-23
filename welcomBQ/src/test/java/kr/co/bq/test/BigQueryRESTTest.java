package kr.co.bq.test;

import org.junit.Test;

import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.bigquery.BigQueryOptions;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.UnsupportedEncodingException;
import java.net.URL;
import java.net.URLEncoder;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.net.ssl.HttpsURLConnection;

import com.google.gson.Gson;


/**
 * BigQuery REST API Test
 * @author gpdlv99
 *
 */
public class BigQueryRESTTest {
	private String projectId = BigQueryOptions.getDefaultInstance().getProjectId();


	/**
	 * Datasets
	 */
	@Test
	public void deleteDataset() {
		String method = "DELETE";
		String url = "/projects/projectId/datasets/datasetId";

		url = url.replaceAll("projectId", projectId);	//projectId 변경
		url = url.replaceAll("datasetId", "testds");	//datasetId 변경

		//REST API 실행
		runREST(method, url, null);
	}

	@Test
	public void getDataset() {
		String method = "GET";
		String url = "/projects/projectId/datasets/datasetId";

		url = url.replaceAll("projectId", projectId);	//projectId 변경
		url = url.replaceAll("datasetId", "testds");	//datasetId 변경

		//REST API 실행
		runREST(method, url, null);
	}

	@Test
	public void insertDataset() {
		String method = "POST";
		String url = "/projects/projectId/datasets";

		url = url.replaceAll("projectId", projectId);	//projectId 변경

		//Parameters
		Map<String, String> subParam =  new HashMap<String, String>(); 
		subParam.put("datasetId", "testds");

		Map<String, Object> param =  new HashMap<String, Object>(); 
		param.put("datasetReference", subParam);
		param.put("description", "test!!!");

		//REST API 실행
		runREST(method, url, mapToJsonStr(param));
	}

	@Test
	public void listDataset() {
		String method = "GET";
		String url = "/projects/projectId/datasets";

		url = url.replaceAll("projectId", projectId);	//projectId 변경

		//REST API 실행
		runREST(method, url, null);
	}

	@Test
	public void updateDataset() {
		String method = "PUT";
		String url = "/projects/projectId/datasets/datasetId";

		url = url.replaceAll("projectId", projectId);	//projectId 변경
		url = url.replaceAll("datasetId", "testds");	//datasetId 변경

		//Parameters
		Map<String, Object> param =  new HashMap<String, Object>(); 
		param.put("description", "update_test");

		//REST API 실행
		runREST(method, url, mapToJsonStr(param));
	}

	@Test
	public void patchDataset() {
		String method = "PATCH";
		String url = "/projects/projectId/datasets/datasetId";

		url = url.replaceAll("projectId", projectId);	//projectId 변경
		url = url.replaceAll("datasetId", "testds");	//datasetId 변경

		//Parameters
		Map<String, Object> param =  new HashMap<String, Object>(); 
		param.put("description", "patch_test!!");

		//REST API 실행
		runREST(method, url, mapToJsonStr(param));
	}

	/**
	 * Jobs
	 */
	@Test
	public void cancelJob() {
		String method = "POST";
		String url = "/projects/projectId/jobs/jobId/cancel";

		url = url.replaceAll("projectId", projectId);	//projectId 변경
		url = url.replaceAll("jobId", "job_y8tD-GAvgBgiQ8au61eTQhUPduL-");	//jobId 변경

		//Parameters (411 오류 회피용)
		Map<String, Object> param =  new HashMap<String, Object>(); 
		param.put("aaa", "aaa");	//임의 값

		//REST API 실행
		runREST(method, url, mapToJsonStr(param));
	}

	@Test
	public void getJob() {
		String method = "GET";
		String url = "/projects/projectId/jobs/jobId";

		url = url.replaceAll("projectId", projectId);	//projectId 변경
		url = url.replaceAll("jobId", "job_IqxQ4BCHBHakdgM5ht8532jgQ-N0");	//jobId 변경

		//REST API 실행
		runREST(method, url, null);
	}

	@Test
	public void getQueryResultsJob() {
		String method = "GET";
		String url = "/projects/projectId/queries/jobId";

		url = url.replaceAll("projectId", projectId);	//projectId 변경
		url = url.replaceAll("jobId", "job_XUYG1TgCxT0xVOv_i4cxQTdWdkHX");	//jobId 변경

		//REST API 실행
		runREST(method, url, null);
	}

	@Test
	public void insertJob() {
		String method = "POST";
		String url = "/projects/projectId/jobs";

		url = url.replaceAll("projectId", projectId);	//projectId 변경

		/***** copy Job 생성 예제 *****/ 
		//Request Body Parameters
		Map<String, String> sourceTable =  new HashMap<String, String>();	//소스테이블 
		sourceTable.put("projectId", projectId);
		sourceTable.put("datasetId", "lab");
		sourceTable.put("tableId", "results_20180319_100329");

		Map<String, String> destinationTable =  new HashMap<String, String>();	//목적테이블 
		destinationTable.put("projectId", projectId);
		destinationTable.put("datasetId", "testds");
		destinationTable.put("tableId", "copy_results_20180319_100329");

		Map<String, Object> copy =  new HashMap<String, Object>(); 
		copy.put("sourceTable", sourceTable);
		copy.put("destinationTable", destinationTable);

		Map<String, Object> configuration =  new HashMap<String, Object>(); 
		configuration.put("copy", copy);

		Map<String, Object> param =  new HashMap<String, Object>(); 
		param.put("configuration", configuration);

		//REST API 실행
		runREST(method, url, mapToJsonStr(param));
	}

	@Test
	public void listJob() {
		String method = "GET";
		String url = "/projects/projectId/jobs";

		url = url.replaceAll("projectId", projectId);	//projectId 변경

		//REST API 실행
		runREST(method, url, null);
	}

	@Test
	public void queryJob() {
		String method = "POST";
		String url = "/projects/projectId/queries";

		url = url.replaceAll("projectId", projectId);	//projectId 변경

		//Parameters
		Map<String, Object> param =  new HashMap<String, Object>(); 
		param.put("query", "SELECT language, sum(requests) as request FROM [lab.pagecounts_20160108_11] group by language having sum(requests) > 999999 order by language");

		//REST API 실행
		runREST(method, url, mapToJsonStr(param));
	}

	/**
	 * Projects
	 */
	@Test
	public void getServiceAccount() {
		String method = "GET";
		String url = "/projects/projectId/serviceAccount";

		url = url.replaceAll("projectId", projectId);	//projectId 변경

		//REST API 실행
		runREST(method, url, null);
	}

	@Test
	public void listProject() {
		String method = "GET";
		String url = "/projects";

		//REST API 실행
		runREST(method, url, null);
	}

	/**
	 * Tabledata
	 */
	@Test
	public void insertAllTabledata() {
		String method = "POST";
		String url = "/projects/projectId/datasets/datasetId/tables/tableId/insertAll";

		url = url.replaceAll("projectId", projectId);	//projectId 변경
		url = url.replaceAll("datasetId", "lab");	//datasetId 변경
		url = url.replaceAll("tableId", "results_20180319_100329");	//tableId 변경

		//Request Body Parameters
		Map<String, String> col =  new HashMap<String, String>();	//column 데이터 
		List<Object> row =  new ArrayList<Object>();				//row 데이터
		Map<String, Object> json =  new HashMap<String, Object>();	//column 데이터 셋

		col.put("language", "ko");
		col.put("title", "한국어타이틀");
		col.put("requests", "10");
		col.put("content_size", "100");
		json.put("json", col);
		row.add(json);

		col = new HashMap<String, String>();
		col.put("language", "jp");
		col.put("title", "jap_title");
		col.put("requests", "20");
		col.put("content_size", "200");
		json = new HashMap<String, Object>();
		json.put("json", col);
		row.add(json);

		col = new HashMap<String, String>();
		col.put("language", "cn");
		col.put("title", "chn_title");
		col.put("requests", "30");
		col.put("content_size", "300");
		json = new HashMap<String, Object>();
		json.put("json", col);
		row.add(json);

		Map<String, Object> param =  new HashMap<String, Object>(); 
		param.put("kind", "bigquery#tableDataInsertAllRequest");
		param.put("rows", row);

		//REST API 실행
		runREST(method, url, mapToJsonStr(param));
	}

	@Test
	public void listTabledata() {
		String method = "GET";
		String url = "/projects/projectId/datasets/datasetId/tables/tableId/data";

		url = url.replaceAll("projectId", projectId);	//projectId 변경
		url = url.replaceAll("datasetId", "lab");	//datasetId 변경
		url = url.replaceAll("tableId", "results_20180319_100329");	//tableId 변경

		//REST API 실행
		runREST(method, url, null);
	}

	/**
	 * Tables
	 */
	@Test
	public void deleteTable() {
		String method = "DELETE";
		String url = "/projects/projectId/datasets/datasetId/tables/tableId";

		url = url.replaceAll("projectId", projectId);	//projectId 변경
		url = url.replaceAll("datasetId", "testds");	//datasetId 변경
		url = url.replaceAll("tableId", "copy_results_20180319_100329");	//tableId 변경

		//REST API 실행
		runREST(method, url, null);
	}

	@Test
	public void getTable() {
		String method = "GET";
		String url = "/projects/projectId/datasets/datasetId/tables/tableId";

		url = url.replaceAll("projectId", projectId);	//projectId 변경
		url = url.replaceAll("datasetId", "testds");	//datasetId 변경
		url = url.replaceAll("tableId", "copy_results_20180319_100329");	//tableId 변경

		//REST API 실행
		runREST(method, url, null);
	}

	@Test
	public void insertTable() {
		String method = "POST";
		String url = "/projects/projectId/datasets/datasetId/tables";

		url = url.replaceAll("projectId", projectId);	//projectId 변경
		url = url.replaceAll("datasetId", "testds");	//datasetId 변경

		//Parameters
		Map<String, String> tableReference =  new HashMap<String, String>(); 
		tableReference.put("projectId", projectId);
		tableReference.put("datasetId", "testds");
		tableReference.put("tableId", "new_test_table");

		Map<String, Object> param =  new HashMap<String, Object>(); 
		param.put("tableReference", tableReference);
		param.put("description", "teble create test!!!");

		//schema 설정
		Map<String, String> col =  new HashMap<String, String>(); 
		List<Object> fields =  new ArrayList<Object>();
		Map<String, Object> schema =  new HashMap<String, Object>();

		col.put("name", "seq");
		col.put("type", "INTEGER");
		col.put("mode", "REQUIRED");
		col.put("description", "일련번호");
		fields.add(col);

		col = new HashMap<String, String>();
		col.put("name", "title");
		col.put("type", "STRING");
		col.put("mode", "REQUIRED");
		col.put("description", "타이틀");
		fields.add(col);

		col = new HashMap<String, String>();
		col.put("name", "desc");
		col.put("type", "STRING");
		col.put("mode", "NULLABLE");
		col.put("description", "설명");
		fields.add(col);

		col = new HashMap<String, String>();
		col.put("name", "price");
		col.put("type", "INTEGER");
		col.put("mode", "NULLABLE");
		col.put("description", "가격");
		fields.add(col);

		schema.put("fields", fields);
		param.put("schema", schema);

		//REST API 실행
		runREST(method, url, mapToJsonStr(param));
	}

	@Test
	public void listTable() {
		String method = "GET";
		String url = "/projects/projectId/datasets/datasetId/tables";

		url = url.replaceAll("projectId", projectId);	//projectId 변경
		url = url.replaceAll("datasetId", "testds");		//datasetId 변경

		//REST API 실행
		runREST(method, url, null);
	}

	@Test
	public void updateTable() {
		String method = "PUT";
		String url = "/projects/projectId/datasets/datasetId/tables/tableId";

		url = url.replaceAll("projectId", projectId);	//projectId 변경
		url = url.replaceAll("datasetId", "testds");	//datasetId 변경
		url = url.replaceAll("tableId", "new_test_table");	//tableId 변경

		//Parameters
		Map<String, Object> param =  new HashMap<String, Object>(); 
		param.put("description", "description is updated by update method");

		//schema 설정  (*** schema 변경시 기존 필드 정보 다 기입해야함 ****)
		Map<String, String> col = new HashMap<String, String>();
		List<Object> fields = new ArrayList<Object>();
		Map<String, Object> schema = new HashMap<String, Object>();

		col.put("name", "seq");
		col.put("type", "INTEGER");
		col.put("mode", "REQUIRED");
		col.put("description", "일련번호");
		fields.add(col);

		col = new HashMap<String, String>();
		col.put("name", "title");
		col.put("type", "STRING");
		col.put("mode", "NULLABLE");
		col.put("description", "타이틀_NULLABLE");
		fields.add(col);

		col = new HashMap<String, String>();
		col.put("name", "desc");
		col.put("type", "STRING");
		col.put("mode", "NULLABLE");
		col.put("description", "설명");
		fields.add(col);

		col = new HashMap<String, String>();
		col.put("name", "price");
		col.put("type", "INTEGER");
		col.put("mode", "NULLABLE");
		col.put("description", "가격");
		fields.add(col);

		col = new HashMap<String, String>();
		col.put("name", "added_col");
		col.put("type", "STRING");
		col.put("mode", "NULLABLE");
		col.put("description", "추가된 컬럼");
		fields.add(col);

		schema.put("fields", fields);
		param.put("schema", schema);

		//REST API 실행
		runREST(method, url, mapToJsonStr(param));
	}

	@Test
	public void patchTable() {
		String method = "PATCH";
		String url = "/projects/projectId/datasets/datasetId/tables/tableId";

		url = url.replaceAll("projectId", projectId);	//projectId 변경
		url = url.replaceAll("datasetId", "testds");	//datasetId 변경
		url = url.replaceAll("tableId", "new_test_table");	//tableId 변경

		//Parameters
		Map<String, Object> param =  new HashMap<String, Object>(); 
		param.put("description", "description is updated by patch method");

		//schema 설정  (*** schema 변경시 기존 필드 정보 다 기입해야함 ****)
		Map<String, String> col = new HashMap<String, String>();
		List<Object> fields = new ArrayList<Object>();
		Map<String, Object> schema = new HashMap<String, Object>();

		col.put("name", "seq");
		col.put("type", "INTEGER");
		col.put("mode", "REQUIRED");
		col.put("description", "일련번호");
		fields.add(col);

		col = new HashMap<String, String>();
		col.put("name", "title");
		col.put("type", "STRING");
		col.put("mode", "NULLABLE");
		col.put("description", "타이틀_NULLABLE");
		fields.add(col);

		col = new HashMap<String, String>();
		col.put("name", "desc");
		col.put("type", "STRING");
		col.put("mode", "NULLABLE");
		col.put("description", "설명");
		fields.add(col);

		col = new HashMap<String, String>();
		col.put("name", "price");
		col.put("type", "INTEGER");
		col.put("mode", "NULLABLE");
		col.put("description", "가격");
		fields.add(col);

		col = new HashMap<String, String>();
		col.put("name", "added_col");
		col.put("type", "STRING");
		col.put("mode", "NULLABLE");
		col.put("description", "추가된 컬럼_patch");
		fields.add(col);

		schema.put("fields", fields);
		param.put("schema", schema);

		//REST API 실행
		runREST(method, url, mapToJsonStr(param));
	}


	/**
	 * BigQuery REST API Run
	 */
	public void runREST(String method, String addUrl, String param) {
		HttpsURLConnection conn;
		OutputStreamWriter writer = null;
		BufferedReader reader = null;
		InputStreamReader isr = null;

		try {
			//OAuth 2.0 인증을 위한 scope 설정 
			List<String> scopes = Arrays.asList("https://www.googleapis.com/auth/bigquery",
					"https://www.googleapis.com/auth/cloud-platform",
					"https://www.googleapis.com/auth/cloud-platform.read-only");	
			//Service Account를 이용한 연결 설정
			// ***** 환경변수 GOOGLE_APPLICATION_CREDENTIALS 설정 필수 *****
			GoogleCredentials credentials = GoogleCredentials.getApplicationDefault().createScoped(scopes);

			//access token 설정
			String accessToken = credentials.refreshAccessToken().getTokenValue();


			// url 설정
			String url = "https://www.googleapis.com/bigquery/v2" + addUrl;

			// GET Parameter 설정 
			if (param != null && param.length() > 0 && method == "GET") url += "?" + param;

			URL objUrl = new URL(url);
			conn = (HttpsURLConnection)objUrl.openConnection();

			if (method.equals("PATCH")) {
				conn.setRequestProperty("X-HTTP-Method-Override", "PATCH");
				conn.setRequestMethod("POST");
			} else {
				conn.setRequestMethod(method);
			}

			conn.setRequestProperty("Authorization", "Bearer " + accessToken);
			conn.setRequestProperty("Content-Type", "application/json");
			conn.setRequestProperty("charset", "utf-8");

			if (param != null && param.length() > 0 && (Arrays.asList("POST", "PUT", "PATCH").contains(method))) {
				conn.setDoOutput(true);
				writer = new OutputStreamWriter(conn.getOutputStream());
				writer.write(param);
				writer.flush();
			}

			//실행 결과 출력
			final int responseCode = conn.getResponseCode();
			System.out.println(String.format("\nSending '%s' request to URL : %s", method, url));
			System.out.println("Response Code : " + responseCode);
			if (responseCode >= 200 && responseCode <= 209)
				isr = new InputStreamReader(conn.getInputStream());
			else
				isr = new InputStreamReader(conn.getErrorStream());

			if (isr != null) {
				reader = new BufferedReader(isr);
				final StringBuffer buffer = new StringBuffer();
				String line;
				while ((line = reader.readLine()) != null) {
					buffer.append(line);
					System.out.println(line);
				}
			}
		} catch(Exception e) {
			e.printStackTrace();
		} finally {
			if (writer != null) try { writer.close(); } catch (Exception ignore) { }
			if (reader != null) try { reader.close(); } catch (Exception ignore) { }
			if (isr != null) try { isr.close(); } catch (Exception ignore) { }
		}
	}

	public String urlEncodeUTF8(String s) {
		try {
			return URLEncoder.encode(s, "UTF-8");
		} catch (UnsupportedEncodingException e) {
			throw new UnsupportedOperationException(e);
		}
	}

	public String mapToParams(Map<String, String > map) {
		StringBuilder paramBuilder = new StringBuilder();
		for (String key : map.keySet()) {
			paramBuilder.append(paramBuilder.length() > 0 ? "&" : "");
			paramBuilder.append(String.format("%s=%s", urlEncodeUTF8(key),
					urlEncodeUTF8(map.get(key).toString())));
		}
		return paramBuilder.toString();
	}

	public String mapToJsonStr(Map<String, Object > map) {
		return new Gson().toJson(map);
	}

}