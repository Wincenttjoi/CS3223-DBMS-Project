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
			
			// TEST5
			Test.doTest(stmt, "select gradyear from student group by gradyear");
//		  gradyear
//	      --------
//			  2019
//			  2020
//			  2021
//			  2022		
			
			// TEST6
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
			
			// TEST7
			Test.doTest(stmt, "select distinct gradyear, count(sid) from student where gradyear < 2021 group by gradyear order by countofsid desc");
//		 gradyear countofsid
//		 -------------------
//		     2020          3
//		     2019          1
			
			// TEST8
			Test.doTest(stmt, "select distinct gradyear, avg(sid) from student where majorid = 10 group by gradyear order by avgofsid desc");
//		 gradyear avgofsid
//		 -----------------
//		     2021     5.00
//		     2022     3.00
			
			// TEST9
			Test.doTest(stmt, "hashjoin select sid, studentid from student, enroll where studentid = sid");
//		    sid studentid
//		    -----------------
//		          1         1
//		          1         1
//		          2         2
//		          4         4
//		          4         4
//		          6         6
			
			// TEST10
			Test.doTest(stmt, "hashjoin select distinct sid, studentid from student, enroll where studentid = sid");
//		    sid studentid
//		    -----------------
//		          1         1
//		          2         2
//		          4         4
//		          6         6
			
			// TEST11
			Test.doTest(stmt, "hashjoin select sid, studentid from student, enroll where studentid = sid order by studentid desc");
//		    sid studentid
//		    -----------------
//		          6         6
//		          4         4
//		          4         4
//		          2         2
//		          1         1
//		          1         1
			
			// TEST12
			Test.doTest(stmt, "hashjoin select majorid, did, deptid, cid, courseid from student, dept, course, section"
					+ " where majorid = did and did = deptid and cid = courseid");
//			 majorid    did deptid    cid courseid
//			 --------------------------------------
//			       30     30     30     62       62
//			       30     30     30     62       62
//			       10     10     10     12       12
//			       10     10     10     12       12
//			       10     10     10     12       12
//			       10     10     10     12       12
//			       10     10     10     12       12
//			       10     10     10     12       12
//			       20     20     20     32       32
//			       20     20     20     32       32
//			       20     20     20     32       32
//			       20     20     20     32       32
//			       20     20     20     32       32
//			       20     20     20     32       32
//			       20     20     20     32       32
//			       20     20     20     32       32
			
			// TEST13
			Test.doTest(stmt, "hashjoin select eid, sid, studentid, sectionid, sectid "
					+ "from student, enroll, section"
					+ " where studentid = sid and sectionid = sectid");
//		    eid    sid studentid sectionid sectid
//		    -----------------------------------------
//		         24      1         1        43     43
//		         14      1         1        13     13
//		         34      2         2        43     43
//		         54      4         4        53     53
//		         44      4         4        33     33
//		         64      6         6        53     53
			
			// TEST14
			Test.doTest(stmt, "hashjoin select count(sid) "
					+ "from student, enroll, section "
					+ "where studentid = sid and sectionid = sectid "
					+ "group by sid");
//		    sid countofsid
//		    ------------------
//		          1          2
//		          2          1
//		          4          2
//		          6          1
			
			// TEST15
			Test.doTest(stmt, "hashjoin select count(sid) "
					+ "from student, enroll, section "
					+ "where studentid = sid and sectionid = sectid "
					+ "group by sid "
					+ "order by countofsid desc");
//		    sid countofsid
//		    ------------------
//		          4          2
//		          1          2
//		          6          1
//		          2          1
		} catch (SQLException e) {
			e.printStackTrace();
		}
		sc.close();
	}

}
