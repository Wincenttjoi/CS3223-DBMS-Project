import java.sql.*;
import java.util.Scanner;
import simpledb.jdbc.embedded.EmbeddedDriver;

// 1. Recreate studentdb using embedded driver.
// 2. Run BonusTest, check if expected output is similar to commented code.
// This class is responsible to show all the tests done for bonus implementations 
public class BonusTest {
   
   public static void main(String[] args) {
	  
	  Scanner sc = new Scanner(System.in);
      System.out.println("Connect> ");
     
      String s = "jdbc:simpledb:studentdb"; // embedded
      Driver d = new EmbeddedDriver();
      
      try (Connection conn = d.connect(s, null);
          Statement stmt = conn.createStatement()) {

    	  // TEST1: No exception thrown even when single table with empty tuple is joined with another table
    	  Test.doTest(stmt, "select sname from student, dept where sname = 'jonah'");
//        sname  
//        ------
    	  
    	  // TEST 2: Select Plan not used repetitively after IndexSelect Plan used
    	  Test.doTest(stmt, "select sid, sname, majorid from student where majorid > 10 and sname != 'bob'");
//    	    sid      sname majorid
//    	    --------------------------
//    	          8        pat      20
//    	          6        kim      20
//    	          4        sue      20
//    	          2        amy      20
//    	          7        art      30
    	  
    	  // TEST 3: Select Plan not used repetitively after IndexSelect Plan used
    	  Test.doTest(stmt, "select sname, sid, majorid from student where majorid > 10 and majorid < 30");
//          sname    sid majorid
//          --------------------------
//                  pat      8      20
//                  kim      6      20
//                  sue      4      20
//                  amy      2      20
    	  
    	  // TEST 4: Select Plan not used repetitively after IndexSelect Plan used
    	  Test.doTest(stmt, "select sname, sid, majorid from student where majorid > 10 and majorid > 20");
//          sname    sid majorid
//          --------------------------
//                  art      7      30
//                  bob      5      30
    	  
    	  // TEST 5: Select Plan not used repetitively after IndexSelect Plan used
    	  Test.doTest(stmt, "select sname, sid, majorid from student where 10 < majorid and majorid > 10");
//          sname    sid majorid
//          --------------------------
//                  pat      8      20
//                  kim      6      20
//                  sue      4      20
//                  amy      2      20
//                  art      7      30
//                  bob      5      30

      }
      catch (SQLException e) {
         e.printStackTrace();
      }
      sc.close();
   }
}