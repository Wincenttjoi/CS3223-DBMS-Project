import java.sql.*;
import java.util.Scanner;
import simpledb.jdbc.embedded.EmbeddedDriver;

/**
 * The following class is for demonstration of single table queries purposes.
 */
public class DemoSingleTable {
   
   public static void main(String[] args) {
	  
	  Scanner sc = new Scanner(System.in);
      System.out.println("Connect> ");
     
      String s = "jdbc:simpledb:demodb"; // embedded
      Driver d = new EmbeddedDriver();
      
      try (Connection conn = d.connect(s, null);
          Statement stmt = conn.createStatement()) {
    	  
    	  Test.doTest(stmt, "select sid, sname, gradyear, majorid from student");
    	  
    	  Test.doTest(stmt, "select cid, title, deptid from course");
    	  
    	  Test.doTest(stmt, "select sectid, courseid, prof, yearoffered from section");
    	  
    	  Test.doTest(stmt, "select eid, studentid, sectionid, grade from enroll");
    	  
    	  // TEST5: Basic query showing duplicates
    	  Test.doTest(stmt, "select did, dname from dept");
//    	    did                     dname
//    	    ---------------------------------
//    	         10                       Law
//    	         20                       Law
//    	         30                       Law
//    	         40                       Law
//    	         50                       Law
//    	         60                Accounting
//    	         70                Accounting
//    	         80                Accounting
//    	         90                Accounting
//    	        100                Accounting
//    	        110                     Music
//    	        120                     Music
//    	        130                     Music
//    	        140                     Music
//    	        150                     Music
//    	        160               Real Estate
//    	        170               Real Estate
//    	        180               Real Estate
//    	        190               Real Estate
//    	        200               Real Estate
//    	        210          Computer Science
//    	        220          Computer Science
//    	        230          Computer Science
//    	        240          Computer Science
//    	        250          Computer Science
//    	        260                 Marketing
//    	        270                 Marketing
//    	        280                 Marketing
//    	        290                 Marketing
//    	        300                 Marketing
//    	        310         Industrial Design
//    	        320         Industrial Design
//    	        330         Industrial Design
//    	        340         Industrial Design
//    	        350         Industrial Design
//    	        360                 Economics
//    	        370                 Economics
//    	        380                 Economics
//    	        390                 Economics
//    	        400                 Economics
//    	        410      Chemical Engineering
//    	        420      Chemical Engineering
//    	        430      Chemical Engineering
//    	        440      Chemical Engineering
//    	        450      Chemical Engineering
//    	        460    Mechanical Engineering
//    	        470    Mechanical Engineering
//    	        480    Mechanical Engineering
//    	        490    Mechanical Engineering
//    	        500    Mechanical Engineering
    	  
    	  // TEST6: Hash Index Used
    	  Test.doTest(stmt, "select sid, sname from student where sid = 5");
//    	    sid                     sname
//    	    ---------------------------------
//    	          5          Layla-Mae Barker
    	  
    	  // TEST7: Hash Index cannot be used for range query
    	  Test.doTest(stmt, "select sid, sname from student where sid < 5");
//    	    sid                     sname
//    	    ---------------------------------
//    	          1               Rick Werner
//    	          2            Shiloh Hartman
//    	          3                 Damon Kim
//    	          4           Cerys Armstrong

    	  
    	  // TEST8: Distinct query
    	  Test.doTest(stmt, "select distinct dname from dept");
//			                     dname
//			--------------------------
//			                Accounting
//			 	  Chemical Engineering
//					  Computer Science
//			      			 Economics
//					 Industrial Design
//			            		   Law
//			      			 Marketing
//				Mechanical Engineering
//			          			 Music
//			    		   Real Estate
    	  
    	  // TEST9: Order By query
    	  Test.doTest(stmt, "select  dname from dept order by dname");
//				          dname
//				--------------------------
//				     Accounting
//				     Accounting
//				     Accounting
//				     Accounting
//				     Accounting
//				Chemical Engineering
//				Chemical Engineering
//				Chemical Engineering
//				Chemical Engineering
//				Chemical Engineering
//				Computer Science
//				Computer Science
//				Computer Science
//				Computer Science
//				Computer Science
//				      Economics
//				      Economics
//				      Economics
//				      Economics
//				      Economics
//				Industrial Design
//				Industrial Design
//				Industrial Design
//				Industrial Design
//				Industrial Design
//				            Law
//				            Law
//				            Law
//				            Law
//				            Law
//				      Marketing
//				      Marketing
//				      Marketing
//				      Marketing
//				      Marketing
//				Mechanical Engineering
//				Mechanical Engineering
//				Mechanical Engineering
//				Mechanical Engineering
//				Mechanical Engineering
//				          Music
//				          Music
//				          Music
//				          Music
//				          Music
//				    Real Estate
//				    Real Estate
//				    Real Estate
//				    Real Estate
//				    Real Estate
    	  
    	  // TEST10: Order By query with keyword asc
    	  Test.doTest(stmt, "select dname from dept order by dname asc");
//				          dname
//				--------------------------
//				     Accounting
//				     Accounting
//				     Accounting
//				     Accounting
//				     Accounting
//				Chemical Engineering
//				Chemical Engineering
//				Chemical Engineering
//				Chemical Engineering
//				Chemical Engineering
//				Computer Science
//				Computer Science
//				Computer Science
//				Computer Science
//				Computer Science
//				      Economics
//				      Economics
//				      Economics
//				      Economics
//				      Economics
//				Industrial Design
//				Industrial Design
//				Industrial Design
//				Industrial Design
//				Industrial Design
//				            Law
//				            Law
//				            Law
//				            Law
//				            Law
//				      Marketing
//				      Marketing
//				      Marketing
//				      Marketing
//				      Marketing
//				Mechanical Engineering
//				Mechanical Engineering
//				Mechanical Engineering
//				Mechanical Engineering
//				Mechanical Engineering
//				          Music
//				          Music
//				          Music
//				          Music
//				          Music
//				    Real Estate
//				    Real Estate
//				    Real Estate
//				    Real Estate
//				    Real Estate
    	  
    	  // TEST11: Distinct and Order By query with keyword desc
    	  Test.doTest(stmt, "select distinct dname from dept order by dname desc");
//			          			 dname
//			--------------------------
//			    		   Real Estate
//			          			 Music
//				Mechanical Engineering
//			      		     Marketing
//			          			   Law
//					 Industrial Design
//					         Economics
//					  Computer Science
//				  Chemical Engineering
//					        Accounting
    	  
    	  
    	  // TEST12: Btree used for range query, 
    	  // Order By of multiple attributes and non-equality >
    	  Test.doTest(stmt, "select sectid, courseid, prof, yearoffered from section "
    	  		+ "where yearoffered >= 2020 order by yearoffered asc, sectid desc");
//    	  sectid courseid                      prof yearoffered
//    	  ------------------------------------------------------
//    	      503      502               Kelvin Wong        2020
//    	      493       72              Moon Geonsik        2020
//    	      453      252                 Ali Heath        2020
//    	      423       62                 Kade Ward        2020
//    	      333       22               Alisa David        2020
//    	      193       32             Layan Bonilla        2020
//    	      153      392           Shelbie Knowles        2020
//    	       83      382              Heena Herman        2020
//    	       73      162               Ayaz Horner        2020
//    	       53      452                 Jan Stott        2020
//    	       13      102              Moon Geonsik        2020
//    	      483      252            Ritchie Cairns        2021
//    	      473      262           Poppy-Mae Boyle        2021
//    	      283       82               Riyad Avila        2021
//    	      273      362              Gabija Baird        2021
//    	      263       72             Marie Farrell        2021
//    	      223      452              Kaydee Parks        2021
//    	      123       52               Zaina Myers        2021
//    	       93      132             Hakim Collins        2021
//    	       23      222               Kelvin Wong        2021
    	 
    	  // TEST13: Multiple non-equality <> < with desc order of attribute
    	  Test.doTest(stmt, "select sectid, courseid, prof, yearoffered from section "
    	  		+ "where yearoffered >= 2020 and yearoffered <> 2021 order by sectid desc");
//    	  sectid courseid                      prof yearoffered
//    	  ------------------------------------------------------
//    	      503      502               Kelvin Wong        2020
//    	      493       72              Moon Geonsik        2020
//    	      453      252                 Ali Heath        2020
//    	      423       62                 Kade Ward        2020
//    	      333       22               Alisa David        2020
//    	      193       32             Layan Bonilla        2020
//    	      153      392           Shelbie Knowles        2020
//    	       83      382              Heena Herman        2020
//    	       73      162               Ayaz Horner        2020
//    	       53      452                 Jan Stott        2020
//    	       13      102              Moon Geonsik        2020
    	  
    	  // TEST14: Showcase student table for next test
    	  Test.doTest(stmt, "select sid, sname, gradyear, majorid from student");
//    	    sid                     sname gradyear majorid
//    	    --------------------------------------------------
//    	          1               Rick Werner     2017     400
//    	          2            Shiloh Hartman     2021     310
//    	          3                 Damon Kim     2020     430
//    	          4           Cerys Armstrong     2018     110
//    	          5          Layla-Mae Barker     2016     240
//    	          6             Tevin Wallace     2016     430
//    	          7            Kamile Wilkins     2021     430
//    	          8             Rohaan Keenan     2016     200
//    	          9         Demi-Leigh Gamble     2016     300
//    	         10           Lilly-Ann Joyce     2018      10
//    	         11              Jadene Oneil     2017     350
//    	         12             Samad Hawkins     2016      40
//    	         13            Scott Goulding     2020      10
//    	         14         Riley-Jay Freeman     2019      40
//    	         15               Sayed Marsh     2020     100
//    	         16            Amiya Caldwell     2017     200
//    	         17             Huzaifa Wiley     2017      20
//    	         18              Jemima Davey     2016     250
//    	         19                 Rhia Bird     2019     110
//    	         20              Romy Delaney     2021      90
//    	         21                Dania Kerr     2017      90
//    	         22            Milana Barrera     2020     470
//    	         23              Ilyas Dudley     2019     150
//    	         24               Aqsa Rivera     2019     350
//    	         25                 Nana Lott     2021     350
//    	         26              Darci Hodson     2021     100
//    	         27             Weronika Bain     2019     380
//    	         28            Callan Derrick     2016      30
//    	         29               Otis Devlin     2017      50
//    	         30             Millie Curtis     2020     480
//    	         31            Sanah Morrison     2018     410
//    	         32              Rania Alcock     2017     140
//    	         33                  Jay Pike     2020     450
//    	         34              Misha Mendez     2016     150
//    	         35            Marcos Whitley     2016     300
//    	         36         Tierney Mcdonnell     2016     170
//    	         37              Maiya Rhodes     2017     290
//    	         38        Darcie-Mae Winters     2019      20
//    	         39                 Marwah Yu     2021     250
//    	         40             Aniela Suarez     2019     120
//    	         41              Corinne Hill     2021     320
//    	         42         Philippa Nicholls     2017     300
//    	         43                 Elif Mayo     2017     200
//    	         44        Tonisha Strickland     2018     150
//    	         45             Wilfred Betts     2016     460
//    	         46            Cassius Spence     2019     390
//    	         47              Amiee Oliver     2017     180
//    	         48             Imaad Wickens     2019      20
//    	         49             Caio Cottrell     2018     290
//    	         50                Ethan Bate     2017     120
    	  
    	  // TEST15: Group By aggn functions, support for avg float
    	  Test.doTest(stmt, "select count(gradyear), min(majorid), max(majorid), avg(sid) "
    	  		+ "from student group by gradyear");
//    	  gradyear countofgradyear minofmajorid maxofmajorid avgofsid
//    	  ------------------------------------------------------------
//    	       2016              11           30          460    21.45
//    	       2017              12           20          400    28.83
//    	       2018               5           10          410    27.60
//    	       2019               9           20          390    31.00
//    	       2020               6           10          480    19.33
//    	       2021               7           90          430    22.86
    	  
    	  // TEST16: Add in integration with order by 
    	  Test.doTest(stmt, "select count(gradyear), min(majorid), max(majorid), avg(sid) "
    	  		+ "from student group by gradyear "
    	  		+ "order by avgofsid desc");
//    	  gradyear countofgradyear minofmajorid maxofmajorid avgofsid
//    	  ------------------------------------------------------------
//    	       2019               9           20          390    31.00
//    	       2017              12           20          400    28.83
//    	       2018               5           10          410    27.60
//    	       2021               7           90          430    22.86
//    	       2016              11           30          460    21.45
//    	       2020               6           10          480    19.33
      }
      catch (SQLException e) {
         e.printStackTrace();
      }
      sc.close();
   }
}