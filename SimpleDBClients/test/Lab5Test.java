import java.sql.*;
import java.util.Scanner;
import simpledb.jdbc.embedded.EmbeddedDriver;

// 1. Recreate studentdb using embedded driver.
// 2. Run Lab5Test, check if expected output is similar to commented code.
// Note: When selecting group fields and aggregate fields together, 
// the order of the fields are not maintained.
public class Lab5Test {

	public static void main(String[] args) {

		Scanner sc = new Scanner(System.in);
		System.out.println("Connect> ");

		String s = "jdbc:simpledb:studentdb"; // embedded
		Driver d = new EmbeddedDriver();

		try (Connection conn = d.connect(s, null); Statement stmt = conn.createStatement()) {

			Test.doTest(stmt, "select max(gradyear) from student");
//        TEST1
//    	  maxofgradyear
//    	  -------------
//    	           2022

			Test.doTest(stmt, "select count(sid), min(gradyear) from student");
//        TEST2
//    	  countofsid minofgradyear
//    	  ------------------------
//    	           9	      2019
		
			Test.doTest(stmt, "select gradyear, count(sid) from student group by gradyear");
//	      TEST3
//	      gradyear countofsid
//		  -------------------
//		      2019          1
//	  	      2020          3
//	              2021          3
//	   	      2022          2

			Test.doTest(stmt, "select majorid, sum(gradyear), count(sid), avg(gradyear) from student group by majorid");
//		  TEST4
//		 majorid sumofgradyear countofsid avgofgradyear
//		 ----------------------------------------------
//		      10          6064          3       2021.33
//		      20          8081          4       2020.25
//		      30          4041          2       2020.50
			
			Test.doTest(stmt, "select gradyear from student group by gradyear");
//		  gradyear
//	      --------
//			  2019
//			  2020
//			  2021
//			  2022		
			
			Test.doTest(stmt, "select gradyear, majorid, count(sid) from student group by gradyear, majorid order by gradyear desc, majorid");
//		 gradyear majorid countofsid
//		 ---------------------------
//		      2022      10         1
//		      2022      20         1
//		      2021      10         2
//		      2021      30         1
//		      2020      20         2
//		      2020      30         1
//		      2019      20         1
			
			Test.doTest(stmt, "select distinct gradyear, count(sid) from student where gradyear < 2021 group by gradyear order by countofsid desc");
//		 gradyear countofsid
//		 -------------------
//		     2020          3
//		     2019          1
			
			Test.doTest(stmt, "select distinct gradyear, avg(sid) from student where majorid = 10 group by gradyear order by avgofsid desc");
//		 gradyear avgofsid
//		 -----------------
//		     2021     5.00
//		     2022     3.00

		} catch (SQLException e) {
			e.printStackTrace();
		}
		sc.close();
	}

}
