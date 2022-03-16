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

    	  // TEST1: Four Way table queries, showing different sname and prof for each course subject
    	  Test.doJoinAlgoTest(stmt, "select sname, majorid, did, deptid, dname, title, cid, courseid, prof "
    	  		+ "from student, dept, course, section "
    	  		+ "where majorid = did and did = deptid and cid = courseid order by dname asc, title asc");
//          sname majorid    did deptid                     dname                          title    cid courseid                      prof
//---------------------------------------------------------------------------------------------------------------------------------------------------
//   Darci Hodson     100    100    100                Accounting              Corporate Finance    102      102              Moon Geonsik
//   Darci Hodson     100    100    100                Accounting              Corporate Finance    102      102               Sheikh Rowe
//   Darci Hodson     100    100    100                Accounting              Corporate Finance    102      102              Amara Tanner
//    Sayed Marsh     100    100    100                Accounting              Corporate Finance    102      102              Moon Geonsik
//    Sayed Marsh     100    100    100                Accounting              Corporate Finance    102      102               Sheikh Rowe
//    Sayed Marsh     100    100    100                Accounting              Corporate Finance    102      102              Amara Tanner
// Kamile Wilkins     430    430    430      Chemical Engineering            Polymer Engineering    462      462             Kelise Berger
//  Tevin Wallace     430    430    430      Chemical Engineering            Polymer Engineering    462      462             Kelise Berger
//      Damon Kim     430    430    430      Chemical Engineering            Polymer Engineering    462      462             Kelise Berger
// Kamile Wilkins     430    430    430      Chemical Engineering                Process Control    472      472           Garfield Hebert
// Kamile Wilkins     430    430    430      Chemical Engineering                Process Control    472      472                   Cole Li
//  Tevin Wallace     430    430    430      Chemical Engineering                Process Control    472      472           Garfield Hebert
//  Tevin Wallace     430    430    430      Chemical Engineering                Process Control    472      472                   Cole Li
//      Damon Kim     430    430    430      Chemical Engineering                Process Control    472      472           Garfield Hebert
//      Damon Kim     430    430    430      Chemical Engineering                Process Control    472      472                   Cole Li
//Layla-Mae Barker     240    240    240          Computer Science              Basic Programming    242      242           Zakariyah Power
//Layla-Mae Barker     240    240    240          Computer Science              Basic Programming    242      242              Wincent Tjoi
//      Marwah Yu     250    250    250          Computer Science                      Databases    252      252               Chase Wyatt
//      Marwah Yu     250    250    250          Computer Science                      Databases    252      252                 Ali Heath
//      Marwah Yu     250    250    250          Computer Science                      Databases    252      252            Ritchie Cairns
//   Jemima Davey     250    250    250          Computer Science                      Databases    252      252               Chase Wyatt
//   Jemima Davey     250    250    250          Computer Science                      Databases    252      252                 Ali Heath
//   Jemima Davey     250    250    250          Computer Science                      Databases    252      252            Ritchie Cairns
//      Marwah Yu     250    250    250          Computer Science                      Databases    252      252                Rohan Drew
//   Jemima Davey     250    250    250          Computer Science                      Databases    252      252                Rohan Drew
// Shiloh Hartman     310    310    310         Industrial Design                     IDE Design    402      402            Nellie Wiggins
//Lilly-Ann Joyce      10     10     10                       Law                   Criminal Law     12       12            Korban Fleming
//Lilly-Ann Joyce      10     10     10                       Law                   Criminal Law     12       12            Gracey Peacock
// Scott Goulding      10     10     10                       Law                   Criminal Law     12       12            Korban Fleming
// Scott Goulding      10     10     10                       Law                   Criminal Law     12       12            Gracey Peacock
//    Otis Devlin      50     50     50                       Law              Equity and Trusts     52       52                Jibril Ali
//    Otis Devlin      50     50     50                       Law              Equity and Trusts     52       52               Zaina Myers
//    Otis Devlin      50     50     50                       Law              Equity and Trusts     52       52             Rosina Lucero
// Callan Derrick      30     30     30                       Law            The Law of Contract     32       32             Layan Bonilla
//  Imaad Wickens      20     20     20                       Law                   law of Torts     22       22               Alisa David
//Darcie-Mae Winters      20     20     20                       Law                   law of Torts     22       22               Alisa David
//  Huzaifa Wiley      20     20     20                       Law                   law of Torts     22       22               Alisa David
//      Rhia Bird     110    110    110                     Music             Introductory Music    112      112               Dina Warner
//      Rhia Bird     110    110    110                     Music             Introductory Music    112      112           Beatriz Shields
//Cerys Armstrong     110    110    110                     Music             Introductory Music    112      112               Dina Warner
//Cerys Armstrong     110    110    110                     Music             Introductory Music    112      112           Beatriz Shields
//   Rania Alcock     140    140    140                     Music                Music and Media    142      142             Beatrix Oneal
//      Elif Mayo     200    200    200               Real Estate   Land Use and Property Rights    202      202             Charlie Dixon
//  Rohaan Keenan     200    200    200               Real Estate   Land Use and Property Rights    202      202             Charlie Dixon
// Amiya Caldwell     200    200    200               Real Estate   Land Use and Property Rights    202      202             Charlie Dixon
//   Amiee Oliver     180    180    180               Real Estate                       RE Taxes    182      182              Ifan Squires

    	  // TEST2: Four Way table queries, with distinct and double order by
    	  Test.doJoinAlgoTest(stmt, "select distinct majorid, did, deptid, dname, title, cid, courseid "
    	  		+ "from student, dept, course, section "
    	  		+ "where majorid = did and did = deptid and cid = courseid order by dname, title");
//    	  majorid    did deptid                     dname                          title    cid courseid
//    	  -----------------------------------------------------------------------------------------------
//    	       100    100    100                Accounting              Corporate Finance    102      102
//    	       430    430    430      Chemical Engineering            Polymer Engineering    462      462
//    	       430    430    430      Chemical Engineering                Process Control    472      472
//    	       240    240    240          Computer Science              Basic Programming    242      242
//    	       250    250    250          Computer Science                      Databases    252      252
//    	       310    310    310         Industrial Design                     IDE Design    402      402
//    	        10     10     10                       Law                   Criminal Law     12       12
//    	        50     50     50                       Law              Equity and Trusts     52       52
//    	        30     30     30                       Law            The Law of Contract     32       32
//    	        20     20     20                       Law                   law of Torts     22       22
//    	       110    110    110                     Music             Introductory Music    112      112
//    	       140    140    140                     Music                Music and Media    142      142
//    	       200    200    200               Real Estate   Land Use and Property Rights    202      202
//    	       180    180    180               Real Estate                       RE Taxes    182      182
      }
      catch (SQLException e) {
         e.printStackTrace();
      }
      sc.close();
   }
}