package kr.co.bq.test;

import static org.junit.Assert.*;

import org.junit.Test;

public class UnionSqlGenerrator {

	@Test
	public void test_UnionSqlGenerate() {
		int maxSize = 101;
		for(int i = 1 ; i < maxSize ; i++) {
			int amt = 1000;
			
			System.out.println("select ");
			System.out.println("    " + i + " as cid , ");
			System.out.println("    " + i * amt + " as amt ");
			if(i == (maxSize-1))
				System.out.println(";");
			else
				System.out.println("union distinct ");  // union next keyword distinct or all
		}	
	}

}
