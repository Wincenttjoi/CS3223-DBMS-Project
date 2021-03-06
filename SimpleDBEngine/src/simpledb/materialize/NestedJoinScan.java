package simpledb.materialize;

import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import simpledb.opt.JoinAlgoSelector;
import simpledb.parse.BadSyntaxException;
import simpledb.parse.QueryData;
import simpledb.query.*;

/**
 * The Scan class for the <i>nested join</i> operator.
 * 
 * @author Edward Sciore
 */
public class NestedJoinScan implements Scan {
	private Scan s1, s2;
	private String fldname1, fldname2;
	private String opr;

	/**
	 * Create a nested join scan having the two underlying scans.
	 * 
	 * @param s1       the LHS scan
	 * @param s2       the RHS scan
	 * @param fldname1 the LHS join field
	 * @param fldname2 the RHS join field
	 * @param opr      the relational operator between join fields
	 */
	public NestedJoinScan(Scan s1, Scan s2, String fldname1, String fldname2, String opr) {
		this.s1 = s1;
		this.s2 = s2;
		this.fldname1 = fldname1;
		this.fldname2 = fldname2;
		this.opr = opr;
		beforeFirst();
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
	 * Returns the string value of the specified field. The value is obtained from
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
	 * Returns true if the specified field is in either of the underlying scans.
	 * 
	 * @see simpledb.query.Scan#hasField(java.lang.String)
	 */
	public boolean hasField(String fldname) {
		return s1.hasField(fldname) || s2.hasField(fldname);
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
