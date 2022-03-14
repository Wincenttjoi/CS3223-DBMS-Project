//package simpledb.materialize;
//
//import simpledb.query.*;
//
///**
// * The Scan class for the <i>hashjoin</i> operator.
// * 
// * @author Moon Geonsik
// */
//public class HashJoinScan implements Scan {
//	private Scan s1, s2;
//	private String fldname1, fldname2;
//	private String opr;
//
//	/**
//	 * Create a hashjoin scan having the two underlying scans.
//	 * 
//	 * @param s1       the LHS scan
//	 * @param s2       the RHS scan
//	 * @param fldname1 the LHS join field
//	 * @param fldname2 the RHS join field
//	 * @param opr      the relational operator between join fields
//	 */
//	public HashJoinScan(Scan s1, Scan s2, String fldname1, String fldname2, String opr) {
//		this.s1 = s1;
//		this.s2 = s2;
//		this.fldname1 = fldname1;
//		this.fldname2 = fldname2;
//		this.opr = opr;
//		beforeFirst();
//	}
//
//	/**
//	 * Position the scan before its first record. In particular, the LHS scan is
//	 * positioned at its first record, and the RHS scan is positioned before its
//	 * first record.
//	 * 
//	 * @see simpledb.query.Scan#beforeFirst()
//	 */
//	public void beforeFirst() {
//		s1.beforeFirst();
//		s1.next();
//		s2.beforeFirst();
//	}
//	
//	/**
//	 * Close both underlying scans.
//	 * 
//	 * @see simpledb.query.Scan#close()
//	 */
//	public void close() {
//		s1.close();
//		s2.close();
//	}
//}
