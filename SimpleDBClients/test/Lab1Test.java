import java.sql.*;
import java.util.Scanner;
import simpledb.jdbc.embedded.EmbeddedDriver;
import simpledb.jdbc.network.NetworkDriver;

public class Lab1Test {
   public static void main(String[] args) {
	  final String DIVIDER = "=========================================";
      Scanner sc = new Scanner(System.in);
      System.out.println("Connect> ");
     
      String s = "jdbc:simpledb:studentdb"; // embedded
      Driver d = new EmbeddedDriver();
      
      try (Connection conn = d.connect(s, null);
          Statement stmt = conn.createStatement()) {

    	  System.out.println("TEST1");
    	  String cmd1 = "select sname, sid, majorid from student where majorid = 10";
    	  doQuery(stmt, cmd1);
    	  System.out.println(DIVIDER);
//          sname    sid majorid
//          --------------------------
//                  joe      1      10
//                  max      3      10
//                  lee      9      10
    	  
    	  System.out.println("TEST2");
    	  String cmd2 = "select sname, sid, majorid from student where majorid <> 20";
    	  doQuery(stmt, cmd2);
    	  System.out.println(DIVIDER);
//          sname    sid majorid
//          --------------------------
//                  joe      1      10
//                  max      3      10
//                  bob      5      30
//                  art      7      30
//                  lee      9      10
    	  
    	  System.out.println("TEST3");
    	  String cmd3 = "select sname, sid, majorid from student where majorid > 10";
    	  doQuery(stmt, cmd3);
    	  System.out.println(DIVIDER);
//          sname    sid majorid
//          --------------------------
//                  amy      2      20
//                  sue      4      20
//                  bob      5      30
//                  kim      6      20
//                  art      7      30
//                  pat      8      20
    	  
    	  System.out.println("TEST4");
    	  String cmd4 = "select sname, sid, majorid from student where majorid >= 10";
    	  doQuery(stmt, cmd4);
    	  System.out.println(DIVIDER);
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
    	  
    	  System.out.println("TEST5");
    	  String cmd5 = "select sname, sid, majorid from student where majorid >= 20 and sid > 4";
    	  doQuery(stmt, cmd5);
    	  System.out.println(DIVIDER);
//          sname    sid majorid
//          --------------------------
//                  bob      5      30
//                  kim      6      20
//                  art      7      30
//                  pat      8      20
    	  
    	  System.out.println("TEST6");
    	  String cmd6 = "select sname, sid, majorid from student where majorid >= 20 and sid > 4 and sname != 'bob'";
    	  doQuery(stmt, cmd6);
    	  System.out.println(DIVIDER);
//          sname    sid majorid
//          --------------------------
//                  kim      6      20
//                  art      7      30
//                  pat      8      20
      }
      catch (SQLException e) {
         e.printStackTrace();
      }
      sc.close();
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
      try {
         int howmany = stmt.executeUpdate(cmd);
         System.out.println(howmany + " records processed");
      }
      catch (SQLException e) {
         System.out.println("SQL Exception: " + e.getMessage());
      }
   }
}