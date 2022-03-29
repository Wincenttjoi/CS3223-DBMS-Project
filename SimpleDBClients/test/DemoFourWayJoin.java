import java.sql.*;
import java.util.Scanner;
import simpledb.jdbc.embedded.EmbeddedDriver;

/**
 * The following class is for demonstration of Four Way table queries purposes.
 */
public class DemoFourWayJoin {
   
   public static void main(String[] args) {
	  
	  Scanner sc = new Scanner(System.in);
      System.out.println("Connect> ");
     
      String s = "jdbc:simpledb:demodb"; // embedded
      Driver d = new EmbeddedDriver();
      
      try (Connection conn = d.connect(s, null);
          Statement stmt = conn.createStatement()) {
    	  
    	  // For querying all tables in demodb
//    	  Test.doQuery(stmt, "select SId, SName, MajorId, GradYear from student");
//    	  Test.doQuery(stmt, "select EId, StudentId, SectionId, Grade from enroll where grade = 'A-'");
//    	  Test.doQuery(stmt, "select DId, DName from dept");
//    	  Test.doQuery(stmt, "select SectId, courseid, prof, yearoffered from section");
//    	  Test.doQuery(stmt, "select CId, Title, DeptId from course");
    	  
    	  String query;

    	  // TEST1: Four Way table queries, with join order to showcase all successful indexjoin. 
    	  // 		Also note the yearOffered = gradyear isn't duplicated in select
    	  query = "select grade, studentid, sid, title, courseid, cid, majorid, yearoffered, gradyear "
    	  		+ "from enroll, student, section, course "
        	  	+ "where grade = 'A-' and studentid = sid and courseid > majorid and courseid = cid and yearOffered = gradyear "
          	  	+ "order by title";
    	  Test.doTest(stmt, query);
    	  Test.doJoinAlgoTest(stmt, query);
    	  //    	  grade studentid    sid                          title courseid    cid majorid yearoffered gradyear
//    	  ---------------------------------------------------------------------------------------------------
//    	      A-        14     14              Basic Programming      242    242      40        2019     2019
//    	      A-        24     24               Brand Management      382    382     350        2019     2019
//    	      A-        14     14               Brand Management      382    382      40        2019     2019
//    	      A-        14     14              Corporate Finance      102    102      40        2019     2019
//    	      A-        26     26                 Data Structure      222    222     100        2021     2021
//    	      A-        26     26                      Databases      252    252     100        2021     2021
//    	      A-        14     14                      Databases      252    252      40        2019     2019
//    	      A-        46     46                Economic Policy      442    442     390        2019     2019
//    	      A-        24     24                Economic Policy      442    442     350        2019     2019
//    	      A-        14     14                Economic Policy      442    442      40        2019     2019
//    	      A-        14     14              Equity and Trusts       52     52      40        2019     2019
//    	      A-        14     14              Financial Markets       72     72      40        2019     2019
//    	      A-         3      3                Fluid Mechanics      502    502     430        2020     2020
//    	      A-        46     46                     IDE Design      402    402     390        2019     2019
//    	      A-        24     24                     IDE Design      402    402     350        2019     2019
//    	      A-        14     14                     IDE Design      402    402      40        2019     2019
//    	      A-        14     14   Land Use and Property Rights      202    202      40        2019     2019
//    	      A-         7      7                  Legal Studies      452    452     430        2021     2021
//    	      A-         3      3                  Legal Studies      452    452     430        2020     2020
//    	      A-        41     41                  Legal Studies      452    452     320        2021     2021
//    	      A-        26     26                  Legal Studies      452    452     100        2021     2021
//    	      A-        26     26                Market Research      262    262     100        2021     2021
//    	      A-        26     26                    Performance      132    132     100        2021     2021
//    	      A-        45     45                Process Control      472    472     460        2016     2016
//    	      A-        45     45                Process Control      472    472     460        2016     2016
//    	      A-        32     32           Quantitative Methods      302    302     140        2017     2017
//    	      A-        14     14                       RE Taxes      182    182      40        2019     2019
//    	      A-        14     14        Residential Real Estate      162    162      40        2019     2019
//    	      A-        24     24              Retail Management      372    372     350        2019     2019
//    	      A-        32     32              Retail Management      372    372     140        2017     2017
//    	      A-        32     32              Retail Management      372    372     140        2017     2017
//    	      A-        14     14              Retail Management      372    372      40        2019     2019
//    	      A-        41     41          Sustainable Marketing      362    362     320        2021     2021
//    	      A-        26     26          Sustainable Marketing      362    362     100        2021     2021
//    	      A-        45     45                 Thermodynamics      512    512     460        2016     2016
    	  
    	  
    	  // TEST2: Same as Test 1, but with inequality conditions (check indexjoin, nestedjoin, mergejoin)
    	  query = "select grade, studentid, sid, title, courseid, cid, majorid, yearoffered, gradyear "
    			+ "from enroll, student, section, course "
      	  		+ "where grade = 'A-' and studentid = sid and courseid > majorid and courseid = cid and yearOffered < gradyear and gradYear = 2019 "
      	  		+ "order by title";
    	  Test.doTest(stmt, query);
    	  Test.doJoinAlgoTest(stmt, query);
    	  
//    	  grade studentid    sid                          title courseid    cid majorid yearoffered gradyear
//    	  ---------------------------------------------------------------------------------------------------
//    	      A-        14     14              Basic Programming      242    242      40        2016     2019
//    	      A-        14     14              Corporate Finance      102    102      40        2018     2019
//    	      A-        14     14                      Databases      252    252      40        2018     2019
//    	      A-        14     14               Decision Science      332    332      40        2016     2019
//    	      A-        14     14              Equity and Trusts       52     52      40        2016     2019
//    	      A-        14     14                Fluid Mechanics      502    502      40        2018     2019
//    	      A-        24     24                Fluid Mechanics      502    502     350        2018     2019
//    	      A-        46     46                Fluid Mechanics      502    502     390        2018     2019
//    	      A-        14     14             Introductory Music      112    112      40        2017     2019
//    	      A-        14     14             Introductory Music      112    112      40        2016     2019
//    	      A-        14     14                Music and Media      142    142      40        2018     2019
//    	      A-        14     14                    Performance      132    132      40        2016     2019
//    	      A-        24     24            Polymer Engineering      462    462     350        2018     2019
//    	      A-        46     46            Polymer Engineering      462    462     390        2018     2019
//    	      A-        14     14            Polymer Engineering      462    462      40        2018     2019
//    	      A-        14     14                Process Control      472    472      40        2016     2019
//    	      A-        14     14                Process Control      472    472      40        2016     2019
//    	      A-        24     24                Process Control      472    472     350        2016     2019
//    	      A-        24     24                Process Control      472    472     350        2016     2019
//    	      A-        46     46                Process Control      472    472     390        2016     2019
//    	      A-        46     46                Process Control      472    472     390        2016     2019
//    	      A-        14     14           Quantitative Methods      302    302      40        2017     2019
//    	      A-        14     14              Retail Management      372    372      40        2017     2019
//    	      A-        24     24              Retail Management      372    372     350        2017     2019
//    	      A-        24     24              Retail Management      372    372     350        2017     2019
//    	      A-        14     14              Retail Management      372    372      40        2017     2019
//    	      A-        46     46                 Thermodynamics      512    512     390        2016     2019
//    	      A-        24     24                 Thermodynamics      512    512     350        2016     2019
//    	      A-        14     14                 Thermodynamics      512    512      40        2016     2019
    	  
    	  // TEST3: Four Way table queries, with group by and order by
    	  query = "select title, count(sid) from enroll, student, section, course "
        	  		+ "where grade = 'A-' and studentid = sid and courseid > majorid and courseid = cid and yearOffered = gradyear "
          	  		+ "group by title "
        	  		+ "order by title";
    	  Test.doTest(stmt, query);
    	  Test.doJoinAlgoTest(stmt, query);    	  
//			title countofsid
//			------------------------------------------
//			Basic Programming          1
//			Brand Management          2
//			Corporate Finance          1
//			Data Structure          1
//			Databases          2
//			Economic Policy          3
//			Equity and Trusts          1
//			Financial Markets          1
//			Fluid Mechanics          1
//			IDE Design          3
//			Land Use and Property Rights          1
//			Legal Studies          4
//			Market Research          1
//			Performance          1
//			Process Control          2
//			Quantitative Methods          1
//			RE Taxes          1
//			Residential Real Estate          1
//			Retail Management          4
//			Sustainable Marketing          2
//			 Thermodynamics          1
      }
      catch (SQLException e) {
         e.printStackTrace();
      }
      sc.close();
   }
}