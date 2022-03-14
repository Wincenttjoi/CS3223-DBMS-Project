import java.sql.*;
import java.util.Scanner;
import simpledb.jdbc.embedded.EmbeddedDriver;

// 1. Recreate studentdb using embedded driver.
// 2. Run Lab5Test, check if expected output is similar to commented code.
// Note: When selecting group fields and aggregate fields together, 
// the order of the fields are not maintained.
public class Lab5Test {
	private static final String DIVIDER = "=========================================";
	private static int testNumber = 0;

	public static void main(String[] args) {

		Scanner sc = new Scanner(System.in);
		System.out.println("Connect> ");

		String s = "jdbc:simpledb:studentdb"; // embedded
		Driver d = new EmbeddedDriver();

		try (Connection conn = d.connect(s, null); Statement stmt = conn.createStatement()) {

			doTest(stmt, "select max(gradyear) from student");
//        TEST1
//    	  maxofgradyear
//    	  -------------
//    	           2022

			doTest(stmt, "select count(sid), min(gradyear) from student");
//        TEST2
//    	  countofsid minofgradyear
//    	  ------------------------
//    	          10	      2019
		
			doTest(stmt, "select gradyear, count(sid) from student group by gradyear");
//	      TEST3
//	      gradyear countofsid
//		  -------------------
//		      2019          1
//	  	      2020          3
//	          2021          3
//	   	      2022          2

			doTest(stmt, "select majorid, sum(gradyear), count(sid), avg(gradyear) from student group by majorid");
//		  TEST4
//		 majorid sumofgradyear countofsid avgofgradyear
//		 ----------------------------------------------
//		      10          6064          3       2021.33
//		      20          8081          4       2020.25
//		      30          4041          2       2020.50
			doTest(stmt, "select gradyear from student group by gradyear");
//		  gradyear
//	      --------
//			  2019
//			  2020
//			  2021
//			  2022			
			doTest(stmt, "select gradyear, majorid, count(sid) from student group by gradyear, majorid order by gradyear desc, majorid");
//		 gradyear majorid countofsid
//		 ---------------------------
//		      2022      10         1
//		      2022      20         1
//		      2021      10         2
//		      2021      30         1
//		      2020      20         2
//		      2020      30         1
//		      2019      20         1
			doTest(stmt, "select distinct gradyear, count(sid) from student where gradyear < 2021 group by gradyear order by countofsid desc");
//		 gradyear countofsid
//		 -------------------
//		     2020          3
//		     2019          1
			doTest(stmt, "select distinct gradyear, avg(sid) from student where majorid = 10 group by gradyear order by avgofsid desc");
//		 gradyear maxofsid
//		 -----------------
//		     2022        3
//		     2021        9
		} catch (SQLException e) {
			e.printStackTrace();
		}
		sc.close();
	}

	private static void doTest(Statement stmt, String cmd) {
		testNumber++;
		System.out.println("TEST" + testNumber); 
		if (cmd.startsWith("select")) {
			doQuery(stmt, cmd);
		} else {
			doUpdate(stmt, cmd);
		}

		System.out.println(DIVIDER);
	}

	private static void doQuery(Statement stmt, String cmd) {
		try (ResultSet rs = stmt.executeQuery(cmd)) {
			ResultSetMetaData md = rs.getMetaData();
			int numcols = md.getColumnCount();
			int totalwidth = 0;

			// print header
			for (int i = 1; i <= numcols; i++) {
				String fldname = md.getColumnName(i);
				int width = md.getColumnDisplaySize(i);
				totalwidth += width;
				String fmt = "%" + width + "s";
				System.out.format(fmt, fldname);
			}
			System.out.println();
			for (int i = 0; i < totalwidth; i++)
				System.out.print("-");
			System.out.println();

			// print records
			while (rs.next()) {
				for (int i = 1; i <= numcols; i++) {
					String fldname = md.getColumnName(i);
					int fldtype = md.getColumnType(i);
					String fmt = "%" + md.getColumnDisplaySize(i);
					if (fldtype == Types.INTEGER) {
						int ival = rs.getInt(fldname);
						System.out.format(fmt + "d", ival);
					} else if (fldtype == Types.FLOAT) {
						float fval = rs.getFloat(fldname);
						System.out.format(fmt + ".2f", fval);
					} else {
						String sval = rs.getString(fldname);
						System.out.format(fmt + "s", sval);
					}
				}
				System.out.println();
			}
		} catch (SQLException e) {
			System.out.println("SQL Exception: " + e.getMessage());
		}
	}

	private static void doUpdate(Statement stmt, String cmd) {
		testNumber++;
		try {
			int howmany = stmt.executeUpdate(cmd);
			System.out.println(howmany + " records processed");
		} catch (SQLException e) {
			System.out.println("SQL Exception: " + e.getMessage());
		}
	}
}