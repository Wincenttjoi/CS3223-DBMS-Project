package simpledb.materialize;

import java.util.*;

import simpledb.query.*;

/**
 * The Scan class for the <i>sort</i> operator.
 * @author Edward Sciore
 */
/**
 * @author sciore
 *
 */
public class DistinctScan implements Scan {
	private Scan s, currentscan;
	private List<String> fields;
	private List<Constant> prev, curr;
	
	public DistinctScan(Scan s, List<String> fields) {
		this.s = s;
		this.currentscan = s;
		this.fields = fields;
		this.prev = new ArrayList<>();
		this.curr = new ArrayList<>();
	}

	public void beforeFirst() {
		s.beforeFirst();
		currentscan.beforeFirst();
	}

	
	public boolean next() {
		boolean notLast = true;
		while (s.next()) {
			return nextDistinct(notLast);
		}
		return false;
	}
	
	/**
	 * Finds the next distinct record and return true.
	 * Every column fields will be checked to ensure that there is no record
	 * that is equal to the previous record.
	 * If the last record is reached and is a duplication, nextDistinct
	 * will return false.
	 * @param notLast boolean whether the record is last
	 * @return boolean true when next distinct record is found
	 */
	private boolean nextDistinct(boolean notLast) {
		boolean isDifferentRecord = false;
		while (!isDifferentRecord && notLast) {
			if (curr != null) {
				curr.clear();
			}
			
			// Store all record contents
			for (String fieldname : fields) {
				Constant value = s.getVal(fieldname);
				curr.add(value);
			}
			
			// First record means no duplication is possible
			if (prev.isEmpty()) {
				prev.addAll(curr);
				return true;
			}
			
			// Check each field if previous record = current record
			for (int i = 0; i < curr.size(); i++) {
				if (!prev.get(i).equals(curr.get(i))) {
					isDifferentRecord = true;
					break;
				}
			}
		
			// Iterate to next record if current record is the same as previous
			if (!isDifferentRecord) {
				notLast = s.next();
			} else {
				this.prev.clear();
				this.prev.addAll(curr);
			}
		}
		
		return notLast;
	}

	public int getInt(String fldname) {
		return s.getInt(fldname);
	}

	public String getString(String fldname) {
		return s.getString(fldname);
	}

	public Constant getVal(String fldname) {
		return s.getVal(fldname);
	}

	public boolean hasField(String fldname) {
		return s.hasField(fldname);
	}

	public void close() {
		s.close();
	}

}