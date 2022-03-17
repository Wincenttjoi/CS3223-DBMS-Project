package simpledb.materialize;

import java.util.*;

import simpledb.query.*;

/**
 * A comparator for scans.
 * 
 */
public class HashComparator implements Comparator<Scan> {
	private String fldname1, fldname2; 
	private int k;

	/**
	 * Create a comparator using the join field, using the given hash function to
	 * determine the partitions
	 * 
	 * @param fldname1 the LHS join field
	 * @param fldname2 the RHS join field
	 * @param k        modulo size used for the hash function (# of partitions)
	 */
	public HashComparator(String fldname1, String fldname2, int k) {
		this.fldname1 = fldname1;
		this.fldname2 = fldname2;
		this.k = k;
	}

	/**
	 * Compare the current records of the two specified scans. The sort fields are
	 * considered in turn. When a field is encountered for which the records have
	 * different values, those values are used as the result of the comparison. If
	 * the two records have the same values for all sort fields, then the method
	 * returns 0.
	 * 
	 * @param s1 the first scan
	 * @param s2 the second scan
	 * @return the result of comparing each scan's current record according to the
	 *         hashed value of the join field
	 */
	public int compare(Scan s1, Scan s2) {
		// assuming only one join field for each scan
		// check if both records hash to the same partition
		Constant val1 = s1.getVal(fldname1);
		Constant val2 = s2.getVal(fldname2);
		Integer offset1 = val1.hashCode() % k;
		Integer offset2 = val2.hashCode() % k;
		return offset1.compareTo(offset2);
	}
}
