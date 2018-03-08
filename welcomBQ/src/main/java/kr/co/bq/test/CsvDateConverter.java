package kr.co.bq.test;

import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class CsvDateConverter {
	/**
	 * Dec 22 2015 같은 날짜 형식을 인식하지 못함.
	 * @param args
	 */
	public static void main(String[] args) {
		try {
			CsvDateConverter converter = new CsvDateConverter();
			converter.csvFileDateConvert();
		}catch(Exception e) {
			e.printStackTrace();
		}
	}

	
	private void csvFileDateConvert() {
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
