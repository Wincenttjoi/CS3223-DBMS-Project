import java.sql.*;
import java.util.Scanner;
import simpledb.jdbc.embedded.EmbeddedDriver;

/**
 * The following class is for demonstration of two way table queries purposes.
 */
public class DemoTwoWayJoin {
   
   public static void main(String[] args) {
	  
	  Scanner sc = new Scanner(System.in);
      System.out.println("Connect> ");
     
      String s = "jdbc:simpledb:demodb"; // embedded
      Driver d = new EmbeddedDriver();
      
      try (Connection conn = d.connect(s, null);
          Statement stmt = conn.createStatement()) {
    	  
    	  // TEST1: Two Way Join student and dept (equality)
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
    	  
    	  // TEST2: Two Way Join student and dept (less than) -- Look at index, merge, nested
//    	  Test.doJoinAlgoTest(stmt, "select sid, majorid, did, dname from student, dept "
//    	  		+ "where majorid < did and sid = 46");
//    	    sid majorid    did                     dname
//    	    ------------------------------------------------
//    	          1     400    410      Chemical Engineering
//    	          1     400    420      Chemical Engineering
//    	          1     400    430      Chemical Engineering
//    	          1     400    440      Chemical Engineering
//    	          1     400    450      Chemical Engineering
//    	          1     400    460    Mechanical Engineering
//    	          1     400    470    Mechanical Engineering
//    	          1     400    480    Mechanical Engineering
//    	          1     400    490    Mechanical Engineering
//    	          1     400    500    Mechanical Engineering
    	  
//    	  // TEST3: Two Way Join student and dept (less or equal to) -- Look at index, merge, nested
//    	  Test.doJoinAlgoTest(stmt, "select sid, majorid, did, dname from student, dept "
//    	  		+ "where majorid <= did and sid = 46");
    	  
//    	    sid majorid    did                     dname
//    	    ------------------------------------------------
//    	         46     390    390                 Economics
//    	         46     390    400                 Economics
//    	         46     390    410      Chemical Engineering
//    	         46     390    420      Chemical Engineering
//    	         46     390    430      Chemical Engineering
//    	         46     390    440      Chemical Engineering
//    	         46     390    450      Chemical Engineering
//    	         46     390    460    Mechanical Engineering
//    	         46     390    470    Mechanical Engineering
//    	         46     390    480    Mechanical Engineering
//    	         46     390    490    Mechanical Engineering
//    	         46     390    500    Mechanical Engineering
    	  
    	  // TEST4: Two Way Join student and dept(more than) -- Look at index, merge, nested
//    	  Test.doJoinAlgoTest(stmt, "select sid, majorid, did, dname from student, dept "
//    	  		+ "where majorid > did and sid = 50");
//    	    sid majorid    did                     dname
//    	    ------------------------------------------------
//    	         50     120     10                       Law
//    	         50     120     20                       Law
//    	         50     120     30                       Law
//    	         50     120     40                       Law
//    	         50     120     50                       Law
//    	         50     120     60                Accounting
//    	         50     120     70                Accounting
//    	         50     120     80                Accounting
//    	         50     120     90                Accounting
//    	         50     120    100                Accounting
//    	         50     120    110                     Music
  
//    	  // TEST5: Two Way Join student and dept (more than or equal to) -- Look at index, merge, nested
//    	  Test.doJoinAlgoTest(stmt, "select sid, majorid, did, dname from student, dept "
//    	  		+ "where majorid >= did and sid = 50");
//    	    sid majorid    did                     dname
//    	    ------------------------------------------------
//    	         50     120     10                       Law
//    	         50     120     20                       Law
//    	         50     120     30                       Law
//    	         50     120     40                       Law
//    	         50     120     50                       Law
//    	         50     120     60                Accounting
//    	         50     120     70                Accounting
//    	         50     120     80                Accounting
//    	         50     120     90                Accounting
//    	         50     120    100                Accounting
//    	         50     120    110                     Music
//    	         50     120    120                     Music
//    	  
//    	  // TEST6: Two Way Join student and dept (not equal to) -- Only look at nested join
//    	  Test.doTest(stmt, "nestedjoin select sid, majorid, did, dname from student, dept "
//    	  		+ "where majorid <> did and sid = 50");
    	  
//    	    sid majorid    did                     dname
//    	    ------------------------------------------------
//    	         50     120     10                       Law
//    	         50     120     20                       Law
//    	         50     120     30                       Law
//    	         50     120     40                       Law
//    	         50     120     50                       Law
//    	         50     120     60                Accounting
//    	         50     120     70                Accounting
//    	         50     120     80                Accounting
//    	         50     120     90                Accounting
//    	         50     120    100                Accounting
//    	         50     120    110                     Music
//    	         50     120    130                     Music
//    	         50     120    140                     Music
//    	         50     120    150                     Music
//    	         50     120    160               Real Estate
//    	         50     120    170               Real Estate
//    	         50     120    180               Real Estate
//    	         50     120    190               Real Estate
//    	         50     120    200               Real Estate
//    	         50     120    210          Computer Science
//    	         50     120    220          Computer Science
//    	         50     120    230          Computer Science
//    	         50     120    240          Computer Science
//    	         50     120    250          Computer Science
//    	         50     120    260                 Marketing
//    	         50     120    270                 Marketing
//    	         50     120    280                 Marketing
//    	         50     120    290                 Marketing
//    	         50     120    300                 Marketing
//    	         50     120    310         Industrial Design
//    	         50     120    320         Industrial Design
//    	         50     120    330         Industrial Design
//    	         50     120    340         Industrial Design
//    	         50     120    350         Industrial Design
//    	         50     120    360                 Economics
//    	         50     120    370                 Economics
//    	         50     120    380                 Economics
//    	         50     120    390                 Economics
//    	         50     120    400                 Economics
//    	         50     120    410      Chemical Engineering
//    	         50     120    420      Chemical Engineering
//    	         50     120    430      Chemical Engineering
//    	         50     120    440      Chemical Engineering
//    	         50     120    450      Chemical Engineering
//    	         50     120    460    Mechanical Engineering
//    	         50     120    470    Mechanical Engineering
//    	         50     120    480    Mechanical Engineering
//    	         50     120    490    Mechanical Engineering
//    	         50     120    500    Mechanical Engineering

    	  // TEST7: Two Way Join student and dept with order by
//    	  Test.doJoinAlgoTest(stmt, "select sid, majorid, did, dname from student, dept "
//    	  		+ "where majorid = did and sid <= 10 order by majorid");
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
    	  
    	  // TEST8: Two Way Join dept and course with 3 conditions
//    	  Test.doJoinAlgoTest(stmt, "select dname, did, deptid, title from course, dept "
//    	  		+ "where did = deptid and did >= 110 and did <= 250");
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