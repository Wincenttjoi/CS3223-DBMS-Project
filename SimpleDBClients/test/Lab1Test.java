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

    	  doTest(stmt, "select sname, sid, majorid from student where majorid = 10");
// TEST1
//          sname    sid majorid
//          --------------------------
//                  joe      1      10
//                  max      3      10
//                  lee      9      10
    	  
    	  doTest(stmt, "select sname, sid, majorid from student where majorid <> 20");
// TEST2
//          sname    sid majorid
//          --------------------------
//                  joe      1      10
//                  max      3      10
//                  bob      5      30
//                  art      7      30
//                  lee      9      10

    	  doTest(stmt, "select sname, sid, majorid from student where majorid > 10");
// TEST3
//          sname    sid majorid
//          --------------------------
//                  amy      2      20
//                  sue      4      20
//                  bob      5      30
//                  kim      6      20
//                  art      7      30
//                  pat      8      20
    	  
    	  doTest(stmt, "select sname, sid, majorid from student where majorid >= 10");
// TEST4
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
    	  
    	  doTest(stmt, "select sname, sid, majorid from student where majorid <= 20");
// TEST5
//          sname    sid majorid
//          --------------------------

//                  amy      2      20
//                  max      3      10
//                  sue      4      20
//                  kim      6      20
//                  pat      8      20
//                  lee      9      10
    	  
    	  doTest(stmt, "select sname, sid, majorid from student where majorid < 20");
// TEST6
//          sname    sid majorid
//          --------------------------

//                  max      3      10
//                  lee      9      10
    	  
    	  doTest(stmt, "select sname, sid, majorid from student where majorid >= 20 and sid > 4");
// TEST7
//          sname    sid majorid
//          --------------------------
//                  bob      5      30
//                  kim      6      20
//                  art      7      30
//                  pat      8      20
    	  
    	  doTest(stmt, "select sname, sid, majorid from student where majorid >= 20 and sid > 4 and sname != 'bob'");
// TEST8
//          sname    sid majorid
//          --------------------------
//                  kim      6      20
//                  art      7      30
//                  pat      8      20
    	  
    	  doTest(stmt, "select sname, sid, majorid from student where sid <> 4");
// TEST9
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
    	  
    	  doTest(stmt, "select sname, sid, majorid from student where sid != 4");
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
    	  
    	  doTest(stmt, "select sname, sid, majorid from student where sid = 4");
// TEST11
//          sname    sid majorid
//          --------------------------
//          sue      4      20
    	  
    	  doTest(stmt, "select sname, sid, majorid from student where sid > 4");
// TEST12
//          sname    sid majorid
//          --------------------------
//          bob      5      30
//          kim      6      20
//          art      7      30
//          pat      8      20
//          lee      9      10
    	  
    	  doTest(stmt, "select sname, sid, majorid from student where sid >= 4");
// TEST13
//          sname    sid majorid
//          --------------------------
//    	    sue      4      20
//          bob      5      30
//          kim      6      20
//          art      7      30
//          pat      8      20
//          lee      9      10
    	  
    	  doTest(stmt, "select sname, sid, majorid from student where sid < 4");
// TEST14
//          sname    sid majorid
//          --------------------------
//          joe      1      10
//          amy      2      20
//          max      3      10
    	  
    	  doTest(stmt, "select sname, sid, majorid from student where sid <= 4");
// TEST15
//          sname    sid majorid
//          --------------------------
//          joe      1      10
//          amy      2      20
//          max      3      10
//          sue      4      20
    	  
    	  doTest(stmt, "select sname from student, dept where sname = 'jonah'");
// TEST16
//        sname  
//        ------
    	  
    	  doTest(stmt, "select sname from student where sname = 'jonah'");
// TEST17
//        sname  
//        ------

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