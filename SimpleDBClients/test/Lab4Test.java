import java.sql.*;
import java.util.Scanner;
import simpledb.jdbc.embedded.EmbeddedDriver;
import simpledb.opt.JoinAlgoSelector;

// 1. Recreate studentdb using embedded driver.
// 2. Run Lab1Test, check if expected output is similar to commented code.
// Note: Currently only works with basic planner, to be integrated in lab 7
// using heuristic planner.
public class Lab4Test {
   
   public static void main(String[] args) {
	  
	  Scanner sc = new Scanner(System.in);
      System.out.println("Connect> ");
     
      String s = "jdbc:simpledb:studentdb"; // embedded
      Driver d = new EmbeddedDriver();
      
      try (Connection conn = d.connect(s, null);
          Statement stmt = conn.createStatement()) {

//		  For reference    	  
//    	  doQuery(stmt, "select SId, SName, MajorId, GradYear from student");
//    	  doQuery(stmt, "select EId, StudentId, SectionId, Grade from enroll");
//    	  doQuery(stmt, "select DId, DName from dept");
//    	  doQuery(stmt, "select CId, Title, DeptId from course");
//
// ------------------- No indexes ------------------- 
    	  
    	  Test.doJoinAlgoTest(stmt, "select title, deptid, did from course, dept where deptid = did");
// TEST1 - Equal
// Note: indexjoin fails because there's no index
//    	          title deptid    did
//-----------------------------------
//    	     db systems     10     10
//    	      compilers     10     10
//    	       calculus     20     20
//    	        algebra     20     20
//    	         acting     30     30
//    	      elocution     30     30
    	  
    	  Test.doJoinAlgoTest(stmt, "select title, deptid, did from course, dept where deptid < did");
// TEST2 - Less than
// Note: indexjoin fails because there's no index
//    	             title  deptid    did
//-----------------------------------------
//    	         db systems     10     20
//    	          compilers     10     20
//    	         db systems     10     30
//    	          compilers     10     30
//    	           calculus     20     30
//    	            algebra     20     30
    	    	  
    	  Test.doJoinAlgoTest(stmt, "select title, deptid, did from course, dept where deptid <> did");
// TEST3 - Not equal
// Note: indexjoin fails because there's no index
//    	 mergejoin fails because we don't support <>
//          title deptid    did
//-----------------------------------
//     db systems     10     20
//     db systems     10     30
//      compilers     10     20
//      compilers     10     30
//       calculus     20     10
//       calculus     20     30
//        algebra     20     10
//        algebra     20     30
//         acting     30     10
//         acting     30     20
//      elocution     30     10
//      elocution     30     20
    	  Test.doJoinAlgoTest(stmt, "select sname, sid, studentid, grade from student, enroll where sid = studentid");
// TEST4 = Equal
//    	  sname    sid studentid grade
//  ----------------------------------
//          joe      1         1     A
//          joe      1         1     C
//          amy      2         2    B+
//          sue      4         4     B
//          sue      4         4     A
//          kim      6         6     A
    	  Test.doJoinAlgoTest(stmt, "select prof, courseid, cid, title from section, course where courseid = cid");
// TEST5 = Equal
//     		prof courseid    cid                title
//     ----------------------------------------------
//        turing       12     12           db systems
//        turing       12     12           db systems
//        newton       32     32             calculus
//      einstein       32     32             calculus
//        brando       62     62            elocution  

// ------------------- Hash index on sid ------------------- 

<<<<<<< HEAD
    	  Test.doTest(stmt, "select sname, sid, studentid, grade from student, enroll where studentid = sid indexjoin");
// TEST6 - Equal
=======
    	  Test.doTest(stmt, "indexjoin select sname, sid, studentid, grade from student, enroll where studentid = sid");
// TEST4 - Equal
>>>>>>> main
//          sname sid    studentid grade
//          --------------------------
//          joe      1         1     A
//          joe      1         1     C
//          amy      2         2    B+
//          sue      4         4     B
//          sue      4         4     A
//          kim      6         6     A
    	  
// ------------------- BTree index on majorid ------------------- 

<<<<<<< HEAD
    	  Test.doTest(stmt, "select sname, sid, majorid, did from student, dept where majorid = did indexjoin");
// TEST7 - Equal 
=======
    	  Test.doTest(stmt, "indexjoin select sname, sid, majorid, did from student, dept where majorid = did");
// TEST5 - Equal 
>>>>>>> main
//                  sname sid  majorid   did
//          ---------------------------------
//                  lee      9      10     10
//                  max      3      10     10
//                  joe      1      10     10
//                  pat      8      20     20
//                  kim      6      20     20
//                  sue      4      20     20
//                  amy      2      20     20
//                  art      7      30     30
//                  bob      5      30     30
    	  
<<<<<<< HEAD
    	  Test.doTest(stmt, "select sname, sid, majorid, did from student, dept where majorid < did indexjoin");
// TEST8 - Less than (lhs index) 
=======
    	  Test.doTest(stmt, "indexjoin select sname, sid, majorid, did from student, dept where majorid < did");
// TEST6 - Less than (lhs index) 
>>>>>>> main
//          sname    sid majorid    did
//          ---------------------------------
//                  joe      1      10     20
//                  max      3      10     20
//                  lee      9      10     20
//                  joe      1      10     30
//                  amy      2      20     30
//                  max      3      10     30
//                  sue      4      20     30
//                  kim      6      20     30
//                  pat      8      20     30
//                  lee      9      10     30
    	  
<<<<<<< HEAD
    	  Test.doTest(stmt, "select sname, sid, majorid, did from student, dept where did > majorid indexjoin");
// TEST9 - Greater than (rhs index) 
=======
    	  Test.doTest(stmt, "indexjoin select sname, sid, majorid, did from student, dept where did > majorid");
// TEST7 - Greater than (rhs index) 
>>>>>>> main
//          sname    sid majorid    did
//          ---------------------------------
//                  lee      9      10     20
//                  max      3      10     20
//                  joe      1      10     20
//                  lee      9      10     30
//                  max      3      10     30
//                  joe      1      10     30
//                  pat      8      20     30
//                  kim      6      20     30
//                  sue      4      20     30
//                  amy      2      20     30
    	  
<<<<<<< HEAD
    	  Test.doTest(stmt, "select sname, sid, majorid, did from student, dept where majorid > did indexjoin");
// TEST10 - Greater than (lhs index) 
=======
    	  Test.doTest(stmt, "indexjoin select sname, sid, majorid, did from student, dept where majorid > did");
// TEST8 - Greater than (lhs index) 
>>>>>>> main
//                  sname sid  majorid   did
//          ---------------------------------
//                  lee      9      10     10
//                  max      3      10     10
//                  joe      1      10     10
//                  pat      8      20     20
//                  kim      6      20     20
//                  sue      4      20     20
//                  amy      2      20     20
//                  art      7      30     30
//                  bob      5      30     30
    	  
<<<<<<< HEAD
    	  Test.doTest(stmt, "select sname, sid, majorid, did from student, dept where did < majorid indexjoin");
// TEST11 - Less than (rhs index) 
=======
    	  Test.doTest(stmt, "indexjoin select sname, sid, majorid, did from student, dept where did < majorid");
// TEST9 - Less than (rhs index) 
>>>>>>> main
//                  sname sid  majorid   did
//          ---------------------------------
//                  lee      9      10     10
//                  max      3      10     10
//                  joe      1      10     10
//                  pat      8      20     20
//                  kim      6      20     20
//                  sue      4      20     20
//                  amy      2      20     20
//                  art      7      30     30
//                  bob      5      30     30
    	  
<<<<<<< HEAD
    	  Test.doTest(stmt, "select sname, sid, majorid, did from student, dept where did <= majorid indexjoin");
// TEST12 - Less than (rhs index) 
=======
    	  Test.doTest(stmt, "indexjoin select sname, sid, majorid, did from student, dept where did <= majorid and did<=majorid");
// TEST10 - Less than (rhs index) 
>>>>>>> main
//          sname    sid majorid    did
//          ---------------------------------
//                  lee      9      10     10
//                  max      3      10     10
//                  joe      1      10     10
//                  pat      8      20     10
//                  kim      6      20     10
//                  sue      4      20     10
//                  amy      2      20     10
//                  art      7      30     10
//                  bob      5      30     10
//                  pat      8      20     20
//                  kim      6      20     20
//                  sue      4      20     20
//                  amy      2      20     20
//                  art      7      30     20
//                  bob      5      30     20
//                  art      7      30     30
//                  bob      5      30     30
    	  
// ------------------- BTree index on majorid and Hash index on sid ------------------- 

<<<<<<< HEAD
    	  Test.doTest(stmt, "select sname, sid, studentid, grade, majorid, did from "
    	  		+ "student, enroll, dept where studentid = sid and majorid = did indexjoin");
// TEST13 - Equal (hash) and equal (btree)
=======
    	  Test.doTest(stmt, "indexjoin select sname, sid, studentid, grade, majorid, did from "
    	  		+ "student, enroll, dept where studentid = sid and majorid = did");
// TEST11 - Equal (hash) and equal (btree)
>>>>>>> main
//          sname    sid studentid grade majorid    did
//          -------------------------------------------------
//                  joe      1         1     A      10     10
//                  joe      1         1     C      10     10
//                  amy      2         2    B+      20     20
//                  sue      4         4     B      20     20
//                  sue      4         4     A      20     20
//                  kim      6         6     A      20     20
    	  
<<<<<<< HEAD
    	  Test.doTest(stmt, "select sname, sid, studentid, grade, majorid, did from "
      	  		+ "student, enroll, dept where studentid = sid and did > majorid indexjoin");
// TEST14 - Equal (hash) and greater than (btree)
=======
    	  Test.doTest(stmt, "indexjoin select sname, sid, studentid, grade, majorid, did from "
      	  		+ "student, enroll, dept where studentid = sid and did > majorid");
// TEST12 - Equal (hash) and greater than (btree)
>>>>>>> main
//          sname    sid studentid grade majorid    did
//          -------------------------------------------------
//                  joe      1         1     A      10     20
//                  joe      1         1     C      10     20
//                  joe      1         1     A      10     30
//                  joe      1         1     C      10     30
//                  amy      2         2    B+      20     30
//                  sue      4         4     B      20     30
//                  sue      4         4     A      20     30
//                  kim      6         6     A      20     30
    	  
<<<<<<< HEAD
    	  Test.doTest(stmt, "select sname, sid, studentid, grade, majorid, did from "
        	  		+ "student, enroll, dept where studentid = sid and majorid >= did indexjoin");
// TEST15 - Equal (hash)  and greater or equal to (btree)
=======
    	  Test.doTest(stmt, "indexjoin select sname, sid, studentid, grade, majorid, did from "
        	  		+ "student, enroll, dept where studentid = sid and majorid >= did");
// TEST13 - Equal (hash)  and greater or equal to (btree)
>>>>>>> main
//          sname    sid studentid grade majorid    did
//          -------------------------------------------------
//                  joe      1         1     A      10     10
//                  joe      1         1     C      10     10
//                  kim      6         6     A      20     10
//                  sue      4         4     B      20     10
//                  sue      4         4     A      20     10
//                  amy      2         2    B+      20     10
//                  kim      6         6     A      20     20
//                  sue      4         4     B      20     20
//                  sue      4         4     A      20     20
//                  amy      2         2    B+      20     20
    	  
      }
      catch (SQLException e) {
         e.printStackTrace();
      }
      sc.close();
   }
}