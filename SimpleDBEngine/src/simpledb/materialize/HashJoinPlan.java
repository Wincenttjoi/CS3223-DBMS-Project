package simpledb.materialize;

import simpledb.tx.Transaction;
import simpledb.plan.Plan;
import simpledb.query.*;
import simpledb.record.*;

import java.util.*;
import java.util.stream.IntStream;

/**
 * The Plan class for the <i>hashjoin</i> operator.
 * 
 */
public class HashJoinPlan implements Plan {
	private Transaction tx;
	private Plan p1, p2;
	private String fldname1, fldname2;
	private int k;
	private Schema sch1 = new Schema();
	private Schema sch2 = new Schema();
	private Schema finalSch = new Schema();
	private String opr;
	private HashComparator comp1;
	private HashComparator comp2;

	/**
	 * Creates a hashjoin plan for the two specified queries.
	 * 
	 * @param tx       the calling transaction
	 * @param p1       the LHS query plan
	 * @param p2       the RHS query plan
	 * @param fldname1 the LHS join field
	 * @param fldname2 the RHS join field
	 * @param opr      the relational operator between join fields
	 * @param k        the number of partitions
	 */
	public HashJoinPlan(Transaction tx, Plan p1, Plan p2, String fldname1, String fldname2, String opr, int k) {
		this.tx = tx;
		this.p1 = p1;
		this.p2 = p2;
		this.fldname1 = fldname1;
		this.fldname2 = fldname2;
		this.k = k;
		this.opr = opr;
		this.comp1 = new HashComparator(fldname1, fldname1, k);
		this.comp2 = new HashComparator(fldname2, fldname2, k);
		sch1 = p1.schema();
		sch2 = p2.schema();
		finalSch.addAll(p1.schema());
		finalSch.addAll(p2.schema());
	}

	/**
	 * Creates a hashjoin scan for this query. In the partition phase, k number of
	 * partitions (temporary tables) are created, and are passed into HashJoinScan
	 * for joining phase.
	 * 
	 * @see simpledb.plan.Plan#open()
	 */
	public Scan open() {
		Scan s1 = p1.open();
		Scan s2 = p2.open();
		Map<Integer, TempTable> part1 = splitIntoPartitions(s1, fldname1, sch1, comp1);
		Map<Integer, TempTable> part2 = splitIntoPartitions(s2, fldname2, sch2, comp2);
		printPlan();
		return new HashJoinScan(part1, part2, fldname1, fldname2, k);
	}

	/**
	 * Estimates the number of block accesses in the join. The formula is:
	 * 
	 * <pre>
	 * B(hashjoin(p1, p2)) = 2 * (B(p1) + B(p2)) + 1 * (B(p1) + B(p2)) (assuming no recursion)
	 * </pre>
	 * 
	 * @see simpledb.plan.Plan#blocksAccessed()
	 */
	public int blocksAccessed() {
		return 2 * (p1.blocksAccessed() + p2.blocksAccessed()) + 1 * (p1.blocksAccessed() + p2.blocksAccessed());
	}

	/**
	 * Return the number of records in the join. Assuming uniform distribution, the
	 * formula is:
	 * 
	 * <pre>
	 *  R(hashjoin(p1,p2)) = R(p1)*R(p2)/max{V(p1,F1),V(p2,F2)}
	 * </pre>
	 * 
	 * @see simpledb.plan.Plan#recordsOutput()
	 */
	public int recordsOutput() {
		int maxvals = Math.max(p1.distinctValues(fldname1), p2.distinctValues(fldname2));
		return (p1.recordsOutput() * p2.recordsOutput()) / maxvals;
	}

	/**
	 * Estimates the number of distinct values for the specified field.
	 * 
	 * @see simpledb.plan.Plan#distinctValues(java.lang.String)
	 */
	public int distinctValues(String fldname) {
		if (p1.schema().hasField(fldname))
			return p1.distinctValues(fldname);
		else
			return p2.distinctValues(fldname);
	}

	/**
	 * Return the schema of the join, which is the union of the schemas of the
	 * underlying queries.
	 * 
	 * @see simpledb.plan.Plan#schema()
	 */
	public Schema schema() {
		return finalSch;
	}

	private Map<Integer, TempTable> splitIntoPartitions(Scan src, String fldname, Schema sch, HashComparator comp) {
		Map<Integer, TempTable> temps = initializePartitions(tx, sch, k);
		src.beforeFirst();
		if (!src.next())
			return temps;
		Constant val = src.getVal(fldname);
		int offset = val.hashCode() % k;
		TempTable currenttemp = temps.get(offset);
		UpdateScan currentscan = currenttemp.open();
		// hashes the current record into a partition
		while (copy(src, currentscan, sch)) {
			// retrieve another partition
			// if the next record hashes to a different partition
			if (comp.compare(src, currentscan) != 0) {
				currentscan.close();
				Constant newVal = src.getVal(fldname);
				int newOffset = newVal.hashCode() % k;
				currenttemp = temps.get(newOffset);
				currentscan = (UpdateScan) currenttemp.open();
			}
		}
		currentscan.close();
		return temps;
	}

	private Map<Integer, TempTable> initializePartitions(Transaction tx, Schema sch, int k) {
		Map<Integer, TempTable> result = new HashMap<>();
		IntStream.range(0, k).forEach(i -> result.put(i, new TempTable(tx, sch)));
		return result;
	}

	private boolean copy(Scan src, UpdateScan dest, Schema sch) {
		dest.insert();
		for (String fldname : sch.fields())
			dest.setVal(fldname, src.getVal(fldname));
		return src.next();
	}

	/**
	 * Prints the plan that is being used.
	 */
	public void printPlan() {
		System.out.println("Hash join plan used on " + fldname1 + opr + fldname2 + " (" + k + " partitions created)");
	}
}
