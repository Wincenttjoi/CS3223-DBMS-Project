package simpledb.materialize;

import simpledb.query.*;

import java.util.*;
import java.util.stream.IntStream;

/**
 * The Scan class for the <i>hashjoin</i> operator.
 * 
 * @author Moon Geonsik
 */
public class HashJoinScan implements Scan {
	private Scan s1, s2;
	private String fldname1, fldname2;
	private String opr;
	private int numPart;
	private Map<Integer, ArrayList<Constant>> hashTables;

	/**
	 * Create a hashjoin scan having the two underlying scans.
	 * 
	 * @param s1       the LHS scan
	 * @param s2       the RHS scan
	 * @param fldname1 the LHS join field
	 * @param fldname2 the RHS join field
	 * @param opr      the relational operator between join fields
	 * @param numPart  the number of partitions
	 */
	public HashJoinScan(Scan s1, Scan s2, String fldname1, String fldname2, String opr, int numPart) {
		this.s1 = s1;
		this.s2 = s2;
		this.fldname1 = fldname1;
		this.fldname2 = fldname2;
		this.opr = opr;
		this.numPart = numPart;
		initializeHashTables();
		beforeFirst();
	}

	private void initializeHashTables() {
		this.hashTables = new HashMap<>();
		IntStream.range(0, numPart).forEach(i -> hashTables.put(i, new ArrayList<Constant>()));
		return;
	}

	/**
	 * Position the scan before its first record. In particular, the LHS scan is
	 * positioned at its first record, and the RHS scan is positioned before its
	 * first record.
	 * 
	 * @see simpledb.query.Scan#beforeFirst()
	 */
	public void beforeFirst() {
		s1.beforeFirst();
		s1.next();
		s2.beforeFirst();
	}
	
	/**
	 * Move the scan to the next record. The method moves to the next RHS record, if
	 * possible. Otherwise, it moves to the next LHS record and the first RHS
	 * record. If there are no more LHS records, the method returns false.
	 * 
	 * @see simpledb.query.Scan#next()
	 */
	public boolean next() {
		do {
			while (s2.next()) {
				if (OprComparator.compare(s1.getVal(fldname1), s2.getVal(fldname2), opr)) {
					return true;
				}
			}
			s2.beforeFirst();
		} while (s1.next());
		return false;
	}

	/**
	 * Close both underlying scans.
	 * 
	 * @see simpledb.query.Scan#close()
	 */
	public void close() {
		s1.close();
		s2.close();
	}
}
