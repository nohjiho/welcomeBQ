package kr.co.bq;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
public class WelcomBqApplication {

	public static void main(String[] args) {
		SpringApplication.run(WelcomBqApplication.class, args);
		System.out.println("Hello Welcome Bank!!");
	}
}
