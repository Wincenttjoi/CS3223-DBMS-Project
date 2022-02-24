package simpledb.materialize;

import simpledb.query.Constant;
import simpledb.query.Scan;

public class DistinctScan implements Scan {
	private Scan s;
	
	public DistinctScan(Scan s) {
		this.s = s;
	}
	
	public void beforeFirst() {
		s.beforeFirst();
		
	}

	public boolean next() {
		while (s.next()) {
			return true;
		}

		return false;
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
