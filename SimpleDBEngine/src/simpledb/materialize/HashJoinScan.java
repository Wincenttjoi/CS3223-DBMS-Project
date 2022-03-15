package simpledb.materialize;

import simpledb.query.*;

import java.util.*;
import java.util.stream.IntStream;

/**
 * The Scan class for the <i>hashjoin</i> operator.
 * 
 */
public class HashJoinScan implements Scan {
	private Scan s1, s2;
	private String fldname1, fldname2;
	private Map<Integer, TempTable> part1, part2;
	private int k;
	private Map<Integer, ArrayList<Scan>> hashTables;
	private HashComparator comp;

	private String opr;
	
	private int partId = 0;

	/**
	 * Create a hashjoin scan having the two underlying scans.
	 * 
	 * @param s1       the LHS scan
	 * @param s2       the RHS scan
	 * @param fldname1 the LHS join field
	 * @param fldname2 the RHS join field
	 * @param opr      the relational operator between join fields
	 * @param k        the number of partitions
	 */
	public HashJoinScan(Scan s1, Scan s2, String fldname1, String fldname2, Map<Integer, TempTable> part1,
			Map<Integer, TempTable> part2, int k, String opr) {
		this.s1 = s1;
		this.s2 = s2;
		this.fldname1 = fldname1;
		this.fldname2 = fldname2;
		this.part1 = part1;
		this.part2 = part2;
		this.k = k;
		this.comp = new HashComparator(fldname1, fldname2, k);
		this.opr = opr;
		s1 = (UpdateScan) part1.get(partId).open();
		s2 = (UpdateScan) part2.get(partId).open();
		initializeHashTables();
		beforeFirst();
	}

	private void initializeHashTables() {
		this.hashTables = new HashMap<>();
		IntStream.range(0, (this.k - 1)).forEach(i -> hashTables.put(i, new ArrayList<Scan>()));
		return;
	}

	/**
	 * Position the scan before its first record. In particular, the LHS scan and
	 * the RHS scan is positioned before its first record.
	 * 
	 * @see simpledb.query.Scan#beforeFirst()
	 */
	public void beforeFirst() {
		s1.beforeFirst();
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
//		populate the hashTables
//		while (s1.next()) {
//			Constant val = s1.getVal(fldname1);
//			int offset = val.hashCode() & (k - 1);
//			this.hashTables.get(offset).add(s1);
//		}
		while (partId <= k) {
			do {
				while (s2.next()) {
					if (comp.compare(s1, s2) == 0) {
						return true;
					}
				}
				s2.beforeFirst();
			} while (s1.next());
			partId ++;
			s1 = (UpdateScan) part1.get(partId).open();
			s2 = (UpdateScan) part2.get(partId).open();
		}
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

	/**
	 * Return the integer value of the specified field. The value is obtained from
	 * whichever scan contains the field.
	 * 
	 * @see simpledb.query.Scan#getInt(java.lang.String)
	 */
	public int getInt(String fldname) {
		if (s1.hasField(fldname))
			return s1.getInt(fldname);
		else
			return s2.getInt(fldname);
	}

	/**
	 * Return the float value of the specified field. The value is obtained from
	 * whichever scan contains the field.
	 * 
	 * @see simpledb.query.Scan#getFloat(java.lang.String)
	 */
	public float getFloat(String fldname) {
		if (s1.hasField(fldname))
			return s1.getFloat(fldname);
		else
			return s2.getFloat(fldname);
	}

	/**
	 * Return the string value of the specified field. The value is obtained from
	 * whichever scan contains the field.
	 * 
	 * @see simpledb.query.Scan#getString(java.lang.String)
	 */
	public String getString(String fldname) {
		if (s1.hasField(fldname))
			return s1.getString(fldname);
		else
			return s2.getString(fldname);
	}

	/**
	 * Return the value of the specified field. The value is obtained from whichever
	 * scan contains the field.
	 * 
	 * @see simpledb.query.Scan#getVal(java.lang.String)
	 */
	public Constant getVal(String fldname) {
		if (s1.hasField(fldname))
			return s1.getVal(fldname);
		else
			return s2.getVal(fldname);
	}

	/**
	 * Return true if the specified field is in either of the underlying scans.
	 * 
	 * @see simpledb.query.Scan#hasField(java.lang.String)
	 */
	public boolean hasField(String fldname) {
		return s1.hasField(fldname) || s2.hasField(fldname);
	}
}
