package simpledb.query;

/**
 * The class that denotes values stored in the database.
 * 
 * @author Edward Sciore
 */
public class Constant implements Comparable<Constant> {
	private Integer ival = null;
	private Float fval = null;
	private String sval = null;

	public Constant(Integer ival) {
		this.ival = ival;
	}

	public Constant(Float fval) {
		this.fval = fval;
	}

	public Constant(String sval) {
		this.sval = sval;
	}

	public int asInt() {
		return ival;
	}

	public float asFloat() {
		return fval;
	}

	public String asString() {
		return sval;
	}

	public boolean equals(Object obj) {
		Constant c = (Constant) obj;
		if (c == null) {
			return false;
		} else if (ival != null) {
			return ival.equals(c.ival);
		} else if (fval != null) {
			return fval.equals(c.fval);
		} else {
			return sval.equals(c.sval);
		}
	}

	public int compareTo(Constant c) {
		if (ival != null) {
			return ival.compareTo(c.ival);
		} else if (fval != null) {
			return fval.compareTo(c.fval);
		} else {
			return sval.compareTo(c.sval);
		}

	}

	public int hashCode() {
		if (ival != null) {
			return ival.hashCode();
		} else if (fval != null) {
			return fval.hashCode();
		} else {
			return sval.hashCode();
		}
	}

	public String toString() {
		if (ival != null) {
			return ival.toString();
		} else if (fval != null) {
			return fval.toString();
		} else {
			return sval.toString();
		}
	}
}
