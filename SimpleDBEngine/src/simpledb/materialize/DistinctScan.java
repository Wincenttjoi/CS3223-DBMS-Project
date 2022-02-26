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
			boolean isDifferentRecord = false;
			while (!isDifferentRecord && notLast) {
				if (curr != null) {
					curr.clear();
				}
				
				for (String fieldname : fields) {
					Constant value = s.getVal(fieldname);
					curr.add(value);
				}
				
				if (prev.isEmpty()) {
					prev.addAll(curr);
					return true;
				}
				
				
				for (int i = 0; i < curr.size(); i++) {
					if (!prev.get(i).equals(curr.get(i))) {
						isDifferentRecord = true;
						break;
					}
					
				}
				
				this.prev.clear();
				this.prev.addAll(curr);
		
				if (!isDifferentRecord) {
					notLast = s.next();
				}
			}
			
			if (notLast) {
				return true;
			} else {
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