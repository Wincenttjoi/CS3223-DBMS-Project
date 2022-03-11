import java.sql.*;
import java.util.Scanner;
import simpledb.jdbc.embedded.EmbeddedDriver;

// 1. Recreate studentdb using embedded driver.
// 2. Run Lab1Test, check if expected output is similar to commented code.
// Note: Currently only works with basic planner, to be integrated in lab 7
// using heuristic planner.
public class Lab6Test {
   
   public static void main(String[] args) {
	  
	  Scanner sc = new Scanner(System.in);
      System.out.println("Connect> ");
     
      String s = "jdbc:simpledb:studentdb"; // embedded
      Driver d = new EmbeddedDriver();
      
      try (Connection conn = d.connect(s, null);
          Statement stmt = conn.createStatement()) {

    	  Test.doTest(stmt, "select majorid from student where majorid = 10");
//        TEST1
//    	  majorid
//    	  --------
//    	        10
//    	        10
//    	        10
    	  
    	  Test.doTest(stmt, "select distinct majorid from student where majorid = 10");
//        TEST2
//    	  majorid
//    	  --------
//    	        10	  
    	  
    	  Test.doTest(stmt, "select gradyear, majorid from student where gradyear = 2021 and majorid = 10");
//        TEST3
//    	  gradyear majorid
//    	  -----------------
//    	       2021      10
//    	       2021      10
    	  
    	  Test.doTest(stmt, "select distinct gradyear, majorid from student where gradyear = 2021 and majorid = 10");
//        TEST4
//    	  gradyear majorid
//    	  -----------------
//    	       2021      10
    	  
    	  Test.doTest(stmt, "select distinct gradyear, majorid from student");
//        TEST5
//    	  gradyear majorid
//    	  -----------------
//    	       2019      20
//    	       2020      20
//    	       2020      30
//    	       2021      10
//    	       2021      30
//    	       2022      10
//    	       2022      20

    	  Test.doTest(stmt, "select distinct majorid from student");
//         TEST6  	  
//    	   majorid
//    	  --------
//    	        10
//    	        20
//    	        30
    	  
    	  Test.doTest(stmt, "select distinct majorid from student order by majorid desc");
//    	  TEST7
//    	  majorid
//    	 --------
//    	       30
//    	       20
//    	       10
    	  
    	  Test.doTest(stmt, "select distinct gradyear, majorid from student order by gradyear desc, majorid desc");
//    	  TEST8
//    	  gradyear majorid
//    	 -----------------
//    	      2022      20
//    	      2022      10
//    	      2021      30
//    	      2021      10
//    	      2020      30
//    	      2020      20
//    	      2019      20   
    	  
    	  Test.doTest(stmt, "select distinct gradyear, majorid from student where gradyear != 2020 order by gradyear desc, majorid desc");
//    	  TEST9
//    	  gradyear majorid
//    	 -----------------
//    	      2022      20
//    	      2022      10
//    	      2021      30
//    	      2021      10
//    	      2019      20
      
    	  Test.doTest(stmt, "select distinct gradyear, majorid from student where majorid <> 20 order by gradyear desc, majorid desc");
//    	  TEST10: NOT WORKING, WAITING FOR NON-EQUALITY INDEX INTEGRATION LAB 
//    	  gradyear majorid
//    	 -----------------
//    	      2022      10
//    	      2021      30
//    	      2021      10
//    	      2020      30    
    	  
    	  Test.doTest(stmt, "select gradyear, majorid, did from student, dept where majorid = did");
//        TEST11
//    	  gradyear majorid    did
//    	  ------------------------
//    	       2021      10     10
//    	       2022      10     10
//    	       2021      10     10
//    	       2020      20     20
//    	       2022      20     20
//    	       2020      20     20
//    	       2019      20     20
//    	       2020      30     30
//    	       2021      30     30
    	  
    	  Test.doTest(stmt, "select distinct gradyear, majorid, did from student, dept where majorid = did");
//        TEST12
//    	  gradyear majorid    did
//    	  ------------------------
//    	       2019      20     20
//    	       2020      20     20
//    	       2020      30     30
//    	       2021      10     10
//    	       2021      30     30
//    	       2022      10     10
//    	       2022      20     20
    	  
    	  Test.doTest(stmt, "select distinct majorid, did from student, dept where majorid = did");
//    	  TEST13
//    	   majorid    did
//    	  ---------------
//    	        10     10
//    	        20     20
//    	        30     30 
      }
      catch (SQLException e) {
         e.printStackTrace();
      }
      sc.close();
   }
}