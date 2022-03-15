import java.sql.*;
import java.util.concurrent.TimeUnit;

import simpledb.opt.JoinAlgoSelector;

// This class is responsible to store general static methods to be used by the
// different LabTests.
public class Test {
   private static final String DIVIDER = "========================================= \n" ;
   private static int testNumber = 0;
   
   public static void doTest(Statement stmt, String cmd) {
	  testNumber++;
 	  System.out.println("TEST" + testNumber);
 	  if (cmd.startsWith("select")) {
 		  doQuery(stmt, cmd);
 	  } else {
 		  doUpdate(stmt, cmd);
 	  }
 	  System.out.println(DIVIDER);
   }

   public static void doQuery(Statement stmt, String cmd) {
	  // Start timer
	  long startTime = System.nanoTime();
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
         // Stops timer
	     long endTime = System.nanoTime();
 	     long duration = endTime - startTime;
 	     if (duration >= 2000000) {
 		     System.out.println("The total time taken for query is: " + TimeUnit.MILLISECONDS.convert(duration, TimeUnit.NANOSECONDS) + "ms");
 	     } else {
 		     System.out.println("The total time taken for query is: " + duration + "ns");
 	     }

      }
      catch (SQLException e) {
         System.out.println("SQL Exception: " + e.getMessage());
      }
   }

   public static void doUpdate(Statement stmt, String cmd) {
	  testNumber++;
      try {
    	 // Start timer
    	 long startTime = System.nanoTime();
         int howmany = stmt.executeUpdate(cmd);
         // Stops timer
	     long endTime = System.nanoTime();
 	     long duration = endTime - startTime;
 	     if (duration >= 2000000) {
 		     System.out.println("The total time taken for update is: " + TimeUnit.MILLISECONDS.convert(duration, TimeUnit.NANOSECONDS) + "ms");
 	     } else {
 		     System.out.println("The total time taken for update is: " + duration + "ns");
 	     }
         System.out.println(howmany + " records processed");
      }
      catch (SQLException e) {
         System.out.println("SQL Exception: " + e.getMessage());
      }
   }
   
   public static void doJoinAlgoTest(Statement stmt, String cmd) {
	  testNumber++;
      for (JoinAlgoSelector selector : JoinAlgoSelector.values()) {
     	  System.out.println("TEST" + testNumber + " " + selector);
    	  doQuery(stmt, cmd + " " + selector);
    	  System.out.println(DIVIDER);
      }
   }
}