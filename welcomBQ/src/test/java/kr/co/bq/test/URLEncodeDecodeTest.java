package kr.co.bq.test;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.stream.Stream;

import org.junit.Ignore;
import org.junit.Test;

public class URLEncodeDecodeTest {

	@Test
	public void test_http_next() {
		String enStr = "%3A%2F%2F";
		String deStr = "";
		try {
			deStr = URLDecoder.decode(enStr, "utf-8");
			
			System.out.println(enStr + "  decode Str : " + deStr);
		} catch (UnsupportedEncodingException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	}
	
	@Test
	public void test_full() {
		String enStr = "https://ssh.cloud.google.com/event/usageTracking/uniqueLoginAttempt?request=%5B%22usageTracking%22%2C%22uniqueLoginAttempt%22%2Cnull%2Cnull%2Ctrue%5D&authuser=0";
		String deStr = "";
		try {
			deStr = URLDecoder.decode(enStr, "utf-8");
			
			System.out.println(enStr + "  decode Str : " + deStr);
		} catch (UnsupportedEncodingException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	@Ignore
	@Test
	public void test_FileRread() {
		String fileName = "C://Users//웰컴저축은행//Desktop//웰컴저축은행//개발관련//구글클라우드//구글클라우드쉘/구글클라우드쉘_요청URL목록_180219.txt";
		
		try {
			Stream<String> fileStream = Files.lines(Paths.get(fileName));
			
			fileStream.forEach(line -> {
				String enStr = line;
				try {
					String deStr = URLDecoder.decode(enStr, "utf-8");
					System.out.println("encoded Str : " + enStr);
					System.out.println("decode Str : " + deStr);
				} catch (UnsupportedEncodingException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			});
		} catch (UnsupportedEncodingException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}
