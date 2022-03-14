import java.sql.*;
import java.util.Scanner;
import simpledb.jdbc.embedded.EmbeddedDriver;

/**
 * The following class is for demonstration of single table queries purposes.
 */
public class DemoTwoWayJoin {
   
   public static void main(String[] args) {
	  
	  Scanner sc = new Scanner(System.in);
      System.out.println("Connect> ");
     
      String s = "jdbc:simpledb:demodb"; // embedded
      Driver d = new EmbeddedDriver();
      
      try (Connection conn = d.connect(s, null);
          Statement stmt = conn.createStatement()) {
    	  
//		  For reference
    	  // TEST1: Single Join student and dept
    	  Test.doJoinAlgoTest(stmt, "select sid, majorid, did, dname from student, dept "
    	  		+ "where majorid = did and sid <= 10");
//    	    sid majorid    did                     dname
//    	    ------------------------------------------------
//    	          1     400    400                 Economics
//    	          2     310    310         Industrial Design
//    	          3     430    430      Chemical Engineering
//    	          4     110    110                     Music
//    	          5     240    240          Computer Science
//    	          6     430    430      Chemical Engineering
//    	          7     430    430      Chemical Engineering
//    	          8     200    200               Real Estate
//    	          9     300    300                 Marketing
//    	         10      10     10                       Law

    	  // TEST2: Single Join student and dept with order by
    	  Test.doJoinAlgoTest(stmt, "select sid, majorid, did, dname from student, dept "
    	  		+ "where majorid = did and sid <= 10 order by majorid asc");
//    	    sid majorid    did                     dname
//    	    ------------------------------------------------
//    	         10      10     10                       Law
//    	          4     110    110                     Music
//    	          8     200    200               Real Estate
//    	          5     240    240          Computer Science
//    	          9     300    300                 Marketing
//    	          2     310    310         Industrial Design
//    	          1     400    400                 Economics
//    	          6     430    430      Chemical Engineering
//    	          7     430    430      Chemical Engineering
//    	          3     430    430      Chemical Engineering
    	  
    	  // TEST3: Single Join dept and course with 3 conditions
    	  Test.doJoinAlgoTest(stmt, "select dname, did, deptid, title from course, dept "
    	  		+ "where did = deptid and did >= 110 and did <= 250");
//		          dname    did deptid                          title
//		-----------------------------------------------------------------------
//		          Music    110    110             Introductory Music
//		          Music    120    120         Composition and Theory
//		          Music    130    130                    Performance
//		          Music    140    140                Music and Media
//		          Music    150    150                      Samplings
//		    Real Estate    160    160        Residential Real Estate
//		    Real Estate    170    170                          Legal
//		    Real Estate    180    180                       RE Taxes
//		    Real Estate    190    190                  RE Technology
//		    Real Estate    200    200   Land Use and Property Rights
//		Computer Science    210    210               Operating System
//		Computer Science    220    220                 Data Structure
//		Computer Science    230    230                      Algorithm
//		Computer Science    240    240              Basic Programming
//		Computer Science    250    250                      Databases	
    	  
    	  
      }
      catch (SQLException e) {
         e.printStackTrace();
      }
      sc.close();
   }
}