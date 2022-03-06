import java.sql.*;
import java.util.Scanner;
import simpledb.jdbc.embedded.EmbeddedDriver;
import simpledb.opt.JoinAlgoSelector;

// 1. Recreate studentdb using embedded driver.
// 2. Run Lab1Test, check if expected output is similar to commented code.
// Note: Currently only works with basic planner, to be integrated in lab 7
// using heuristic planner.
public class Lab4Test {
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
//    	  doQuery(stmt, "select EId, StudentId, SectionId, Grade from enroll");
//    	  doQuery(stmt, "select DId, DName from dept");
//    	  doQuery(stmt, "select CId, Title, DeptId from course");
//
// ------------------- No indexes ------------------- 
    	  
    	  doJoinAlgoTest(stmt, "select title, deptid, did from course, dept where deptid = did");
// TEST1 - Equal
// Note: indexjoin fails because there's no index
//    	          title deptid    did
//-----------------------------------
//    	     db systems     10     10
//    	      compilers     10     10
//    	       calculus     20     20
//    	        algebra     20     20
//    	         acting     30     30
//    	      elocution     30     30
    	  
    	  doJoinAlgoTest(stmt, "select title, deptid, did from course, dept where deptid < did");
// TEST2 - Less than
// Note: indexjoin fails because there's no index + range search not supported
//    	             title  deptid    did
//-----------------------------------------
//    	         db systems     10     20
//    	          compilers     10     20
//    	         db systems     10     30
//    	          compilers     10     30
//    	           calculus     20     30
//    	            algebra     20     30
    	    	  
    	  doJoinAlgoTest(stmt, "select title, deptid, did from course, dept where deptid <> did");
// TEST3 - Not equal
// Note: indexjoin fails because there's no index + range search not supported
//    	 mergejoin fails because we don't support <>
//    	              title deptid    did
//-----------------------------------------
//    	         db systems     10     20
//    	          compilers     10     20
//    	         db systems     10     30
//    	          compilers     10     30
//    	           calculus     20     30
//    	            algebra     20     30
    	    	  
// ------------------- Hash index on sid ------------------- 

    	  doTest(stmt, "select sname, sid, studentid, grade from student, enroll where studentid = sid indexjoin");
// TEST4 - Equal
//          sname sid    majorid grade
//          --------------------------
//          joe      1         1     A
//          joe      1         1     C
//          amy      2         2    B+
//          sue      4         4     B
//          sue      4         4     A
//          kim      6         6     A
    	  
// ------------------- BTree index on majorid ------------------- 

    	  doTest(stmt, "select sname, sid, majorid, did from student, dept where majorid = did indexjoin");
// TEST5 - Equal 
//                  sname sid  majorid   did
//          ---------------------------------
//                  lee      9      10     10
//                  max      3      10     10
//                  joe      1      10     10
//                  pat      8      20     20
//                  kim      6      20     20
//                  sue      4      20     20
//                  amy      2      20     20
//                  art      7      30     30
//                  bob      5      30     30
    	  
// ------------------- BTree index on majorid and Hash index on sid ------------------- 

    	  doTest(stmt, "select sname, sid, studentid, grade, majorid, did from "
    	  		+ "student, enroll, dept where studentid = sid and majorid = did indexjoin");
// TEST6 - Equal (hash) and equal (btree)
//          sname    sid studentid grade majorid    did
//          -------------------------------------------------
//                  joe      1         1     A      10     10
//                  joe      1         1     C      10     10
//                  amy      2         2    B+      20     20
//                  sue      4         4     B      20     20
//                  sue      4         4     A      20     20
//                  kim      6         6     A      20     20
    	  
    	  doTest(stmt, "select sname, sid, studentid, grade, majorid, did from "
      	  		+ "student, enroll, dept where studentid = sid and majorid < did indexjoin");
// TEST7 - Equal (hash) and less than (btree)
//          sname    sid studentid grade majorid    did
//          -------------------------------------------------
//                  joe      1         1     A      10     20
//                  joe      1         1     C      10     20
//                  joe      1         1     A      10     30
//                  joe      1         1     C      10     30
//                  amy      2         2    B+      20     30
//                  sue      4         4     B      20     30
//                  sue      4         4     A      20     30
//                  kim      6         6     A      20     30
    	  
    	  doTest(stmt, "select sname, sid, studentid, grade, majorid, did from "
        	  		+ "student, enroll, dept where studentid = sid and majorid <> did indexjoin");
// TEST8 - Equal (hash)  and not equal (btree)
//          sname    sid studentid grade majorid    did
//          -------------------------------------------------
//                  amy      2         2    B+      20     10
//                  sue      4         4     B      20     10
//                  sue      4         4     A      20     10
//                  kim      6         6     A      20     10
//                  joe      1         1     A      10     20
//                  joe      1         1     C      10     20
//                  joe      1         1     A      10     30
//                  joe      1         1     C      10     30
//                  amy      2         2    B+      20     30
//                  sue      4         4     B      20     30
//                  sue      4         4     A      20     30
//                  kim      6         6     A      20     30
    	  
      }
      catch (SQLException e) {
         e.printStackTrace();
      }
      sc.close();
   }
   
   private static void doTest(Statement stmt, String cmd) {
	  testNumber++;
 	  System.out.println("TEST" + testNumber);
 	  if (cmd.startsWith("select")) {
 		  doQuery(stmt, cmd);
 	  } else {
 		  doUpdate(stmt, cmd);
 	  }
 	  
 	  System.out.println(DIVIDER);
   }
   
   private static void doJoinAlgoTest(Statement stmt, String cmd) {
	  testNumber++;
      for (JoinAlgoSelector selector : JoinAlgoSelector.values()) {
     	  System.out.println("TEST" + testNumber + " " + selector);
    	  doQuery(stmt, cmd + " " + selector);
    	  System.out.println(DIVIDER);
      }
 	  System.out.println(DIVIDER);
   }

   private static void doQuery(Statement stmt, String cmd) {
      try (ResultSet rs = stmt.executeQuery(cmd)) {
         ResultSetMetaData md = rs.getMetaData();
         int numcols = md.getColumnCount();
         int totalwidth = 0;

         // print header
         for(int i=1; i<=numcols; i++) {
            String fldname = md.getColumnName(i);
            int width = md.getColumnDisplaySize(i);
            totalwidth += width;
            String fmt = "%" + width + "s";
            System.out.format(fmt, fldname);
         }
         System.out.println();
         for(int i=0; i<totalwidth; i++)
            System.out.print("-");
         System.out.println();

         // print records
         while(rs.next()) {
            for (int i=1; i<=numcols; i++) {
               String fldname = md.getColumnName(i);
               int fldtype = md.getColumnType(i);
               String fmt = "%" + md.getColumnDisplaySize(i);
               if (fldtype == Types.INTEGER) {
                  int ival = rs.getInt(fldname);
                  System.out.format(fmt + "d", ival);
               }
               else {
                  String sval = rs.getString(fldname);
                  System.out.format(fmt + "s", sval);
               }
            }
            System.out.println();
         }
      }
      catch (SQLException e) {
         System.out.println("SQL Exception: " + e.getMessage());
      }
   }

   private static void doUpdate(Statement stmt, String cmd) {
	  testNumber++;
      try {
         int howmany = stmt.executeUpdate(cmd);
         System.out.println(howmany + " records processed");
      }
      catch (SQLException e) {
         System.out.println("SQL Exception: " + e.getMessage());
      }
   }
}