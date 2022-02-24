package simpledb.materialize;

import simpledb.query.Constant;
import simpledb.query.Scan;

public class DistinctScan implements Scan {
	private Scan s;
	private RecordComparator comp;
	private Scan currentscan = null;
	
	public DistinctScan(Scan s, RecordComparator comp) {
		this.s = s;
		this.comp = comp;
		this.currentscan = null;
	}
	
	public void beforeFirst() {
		s.beforeFirst();
	}

	public boolean next() {
		if (currentscan != null) {
			if (comp.compare(currentscan, s) != 0) {
				currentscan = s;
				s.next();
				return true;
			}
		} else {
			currentscan = s;
			s.next();
			return true;
		}
//		while (s.next()) {
//			if (currentscan == null) {
//				currentscan = s;
//				return true;
//			}
//			else if (comp.compare(s, currentscan) == 0) {
//				currentscan = s;
//				return false;
//			} else {
//				currentscan = s;
//				return true;
//			}
//		}
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
