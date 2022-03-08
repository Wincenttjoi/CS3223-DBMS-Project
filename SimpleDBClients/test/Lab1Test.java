import java.sql.*;
import java.util.Scanner;
import simpledb.jdbc.embedded.EmbeddedDriver;

// 1. Recreate studentdb using embedded driver.
// 2. Run Lab1Test, check if expected output is similar to commented code.
// Note: Currently only works with basic planner, to be integrated in lab 7
// using heuristic planner.
public class Lab1Test {
   private static final String DIVIDER = "=========================================";
   private static int testNumber = 0;
   
   public static void main(String[] args) {
	  
	  Scanner sc = new Scanner(System.in);
      System.out.println("Connect> ");
     
      String s = "jdbc:simpledb:studentdb"; // embedded
      Driver d = new EmbeddedDriver();
      
      try (Connection conn = d.connect(s, null);
          Statement stmt = conn.createStatement()) {
    	  
//		  For reference
//    	  doQuery(stmt, "select SId, SName, MajorId, GradYear from student");

    	  Test.doTest(stmt, "select sname, sid, majorid from student where majorid = 10");
// TEST1
//          sname    sid majorid
//          --------------------------
//                  joe      1      10
//                  max      3      10
//                  lee      9      10
    	  
    	  Test.doTest(stmt, "select sname, sid, majorid from student where majorid <> 20");
// TEST2
//          sname    sid majorid
//          --------------------------
//                  joe      1      10
//                  max      3      10
//                  bob      5      30
//                  art      7      30
//                  lee      9      10

    	  Test.doTest(stmt, "select sname, sid, majorid from student where majorid > 10");
// TEST3
//          sname    sid majorid
//          --------------------------
//                  amy      2      20
//                  sue      4      20
//                  bob      5      30
//                  kim      6      20
//                  art      7      30
//                  pat      8      20
    	  
    	  Test.doTest(stmt, "select sname, sid, majorid from student where 10 <= majorid");
// TEST4
//        sname    sid majorid
//        --------------------------
//                joe      1      10
//                amy      2      20
//                max      3      10
//                sue      4      20
//                bob      5      30
//                kim      6      20
//                art      7      30
//                pat      8      20
//                lee      9      10
    	  
    	  Test.doTest(stmt, "select sname, sid, majorid from student where majorid >= 10");
// TEST5
//          sname    sid majorid
//          --------------------------
//                  joe      1      10
//                  amy      2      20
//                  max      3      10
//                  sue      4      20
//                  bob      5      30
//                  kim      6      20
//                  art      7      30
//                  pat      8      20
//                  lee      9      10
    	  
    	  Test.doTest(stmt, "select sname, sid, majorid from student where majorid <= 20");
// TEST6
//          sname    sid majorid
//          --------------------------
//          lee      9      10
//          max      3      10
//          joe      1      10
//          pat      8      20
//          kim      6      20
//          sue      4      20
//          amy      2      20
    	  
    	  Test.doTest(stmt, "select sname, sid, majorid from student where majorid < 20");
// TEST7
//          sname    sid majorid
//          --------------------------
//          lee      9      10
//          max      3      10
//          joe      1      10
    	  
    	  Test.doTest(stmt, "select sname, sid, majorid from student where majorid >= 20 and sid > 4");
// TEST8
//          sname    sid majorid
//          --------------------------
//                  bob      5      30
//                  kim      6      20
//                  art      7      30
//                  pat      8      20
    	  
    	  Test.doTest(stmt, "select sname, sid, majorid from student where majorid >= 20 and sid > 4 and sname != 'bob'");
// TEST9
//          sname    sid majorid
//          --------------------------
//                  kim      6      20
//                  art      7      30
//                  pat      8      20
    	  
    	  Test.doTest(stmt, "select sname, sid, majorid from student where sid <> 4");
// TEST10
//          sname    sid majorid
//          --------------------------
//          joe      1      10
//          amy      2      20
//          max      3      10
//          bob      5      30
//          kim      6      20
//          art      7      30
//          pat      8      20
//          lee      9      10
    	  
    	  Test.doTest(stmt, "select sname, sid, majorid from student where sid != 4");
// TEST11
//          sname    sid majorid
//          --------------------------
//          joe      1      10
//          amy      2      20
//          max      3      10
//          bob      5      30
//          kim      6      20
//          art      7      30
//          pat      8      20
//          lee      9      10
    	  
    	  Test.doTest(stmt, "select sname, sid, majorid from student where sid = 4");
// TEST12
//          sname    sid majorid
//          --------------------------
//          sue      4      20
    	  
    	  Test.doTest(stmt, "select sname, sid, majorid from student where sid > 4");
// TEST13
//          sname    sid majorid
//          --------------------------
//          bob      5      30
//          kim      6      20
//          art      7      30
//          pat      8      20
//          lee      9      10
    	  
    	  Test.doTest(stmt, "select sname, sid, majorid from student where sid >= 4");
// TEST14
//          sname    sid majorid
//          --------------------------
//    	    sue      4      20
//          bob      5      30
//          kim      6      20
//          art      7      30
//          pat      8      20
//          lee      9      10
    	  
    	  Test.doTest(stmt, "select sname, sid, majorid from student where sid < 4");
// TEST15
//          sname    sid majorid
//          --------------------------
//          joe      1      10
//          amy      2      20
//          max      3      10
    	  
    	  Test.doTest(stmt, "select sname, sid, majorid from student where sid <= 4");
// TEST16
//          sname    sid majorid
//          --------------------------
//          joe      1      10
//          amy      2      20
//          max      3      10
//          sue      4      20
    	  
    	  Test.doTest(stmt, "select sname from student, dept where sname = 'jonah'");
// TEST17
//        sname  
//        ------
    	  
//    	  doTest(stmt, "select sname from student where sname = 'jonah'");
// TEST18
//        sname  
//        ------


      }
      catch (SQLException e) {
         e.printStackTrace();
      }
      sc.close();
   }
}