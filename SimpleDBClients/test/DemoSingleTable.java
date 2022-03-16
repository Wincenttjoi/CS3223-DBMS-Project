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
    	  
    	  // TEST1: Basic query showing duplicates, hash index
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
    	  
    	  // TEST2: Hash Index Used
    	  Test.doTest(stmt, "select did, dname from dept where did = 200");
//    	    did                     dname
//    	    ---------------------------------
//    	        200               Real Estate
    	  
    	  // TEST3: Hash Index cannot be used for range query
    	  Test.doTest(stmt, "select did, dname from dept where did > 200");
//    	    did                     dname
//    	    ---------------------------------
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
    	  
    	  // TEST4: Distinct query
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
    	  
    	  // TEST5: Order By query
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
    	  
    	  // TEST6: Order By query with keyword asc
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
    	  
    	  // TEST7: Distinct and Order By query with keyword desc
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
    	  
    	  // TEST8: Order By of multiple attributes and non-inequality >, yearoffered index used
    	  Test.doTest(stmt, "select sectid, courseid, prof, yearoffered from section "
    	  		+ "where yearoffered > 2019 order by yearoffered asc, sectid desc");
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
    	 
    	  // TEST9: Order By of multiple attributes and non-inequality <> < with index desc order
    	  Test.doTest(stmt, "select sectid, courseid, prof, yearoffered from section "
    	  		+ "where yearoffered < 2020 and courseid <> 332 order by yearoffered desc, sectid desc");
//    	  sectid courseid                      prof yearoffered
//    	  ------------------------------------------------------
//    	      363      142             Beatrix Oneal        2018
//    	      303      252                Rohan Drew        2018
//    	      253      462             Kelise Berger        2018
//    	      173      102               Sheikh Rowe        2018
//    	      113      502             Raihan Baxter        2018
//    	      513      372              Wincent Tjoi        2017
//    	      463      372             Cecil Derrick        2017
//    	      383      302            Haidar Merrill        2017
//    	      313      112           Beatriz Shields        2017
//    	      233       12            Gracey Peacock        2017
//    	      433       52                Jibril Ali        2016
//    	      343      512               Maya Tucker        2016
//    	      323      132              Sianna Reyes        2016
//    	      143      472                   Cole Li        2016
//    	       63      472           Garfield Hebert        2016
//    	       43      112               Dina Warner        2016
//    	       33      242              Wincent Tjoi        2016
    	  
    	  // TODO: Add tests of aggregate function with/without group,
    	  // aggn fn with/without group combined with order by and distinct
      }
      catch (SQLException e) {
         e.printStackTrace();
      }
      sc.close();
   }
}