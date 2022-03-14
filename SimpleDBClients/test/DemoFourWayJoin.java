import java.sql.*;
import java.util.Scanner;
import simpledb.jdbc.embedded.EmbeddedDriver;

/**
 * The following class is for demonstration of single table queries purposes.
 */
public class DemoFourWayJoin {
   
   public static void main(String[] args) {
	  
	  Scanner sc = new Scanner(System.in);
      System.out.println("Connect> ");
     
      String s = "jdbc:simpledb:demodb"; // embedded
      Driver d = new EmbeddedDriver();
      
      try (Connection conn = d.connect(s, null);
          Statement stmt = conn.createStatement()) {
    	  
//		  For reference
    	  // TEST1: Single Join student and dept
    	  Test.doJoinAlgoTest(stmt, "select sid, majorid, did, deptid, dname, title, cid, courseid, prof "
    	  		+ "from student, dept, course, section "
    	  		+ "where majorid = did and did = deptid and cid = courseid");
    	  
    	  
      }
      catch (SQLException e) {
         e.printStackTrace();
      }
      sc.close();
   }
}