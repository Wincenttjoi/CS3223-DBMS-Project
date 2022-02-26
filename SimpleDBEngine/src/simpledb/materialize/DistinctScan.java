package simpledb.materialize;

import java.util.*;

import simpledb.query.*;
import simpledb.record.*;

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
	private Schema sch;
	private RecordComparator comp;
	
	public DistinctScan(Scan s, Schema sch, RecordComparator comp) {
		this.s = s;
		this.currentscan = s;
		this.sch = sch;
		this.comp = comp;
	}

	public void beforeFirst() {
		s.beforeFirst();
	}

	public boolean next() {
		while (s.next()) {
			if (comp.compare(s, currentscan) != 0) {
				currentscan = s;
				return true;
			} else {
				currentscan = s;
				return false;
			}
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