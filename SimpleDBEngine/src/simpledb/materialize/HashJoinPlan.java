//package simpledb.materialize;
//
//import simpledb.tx.Transaction;
//import simpledb.plan.Plan;
//import simpledb.query.*;
//import simpledb.record.*;
//
///**
// * The Plan class for the <i>hashjoin</i> operator.
// * 
// * @author Moon Geonsik
// */
//public class HashJoinPlan implements Plan {
//	private Transaction tx;
//	private Plan p1, p2;
//	private String fldname1, fldname2;
//	private Schema schema = new Schema();
//	private String opr;
//	private int k;
//
//	/**
//	 * Creates a hashjoin plan for the two specified queries.
//	 * 
//	 * @param tx       the calling transaction
//	 * @param p1       the LHS query plan
//	 * @param p2       the RHS query plan
//	 * @param fldname1 the LHS join field
//	 * @param fldname2 the RHS join field
//	 * @param opr      the relational operator between join fields
//	 */
//	public HashJoinPlan(Transaction tx, Plan p1, Plan p2, String fldname1, String fldname2, String opr) {
//		this.tx = tx;
//		this.p1 = p1;
//		this.p2 = p2;
//		this.fldname1 = fldname1;
//		this.fldname2 = fldname2;
//		this.opr = opr;
//		schema.addAll(p1.schema());
//		schema.addAll(p2.schema());
//	}
//
//	/**
//	 * Creates a hash join scan for this query.
//	 * 
//	 * @see simpledb.plan.Plan#open()
//	 */
//	public Scan open() {
//		Scan s1 = p1.open();
//		Scan s2 = p2.open();
//		printPlan();
//		return new HashJoinScan(s1, s2, fldname1, fldname2, opr);
//	}
//
//	/**
//	 * Estimates the number of block accesses in the join. The formula is:
//	 * 
//	 * <pre>
//	 * B(hashjoin(p1, p2)) = 2 * (B(p1) + B(p2)) + 1 * (B(p1) + B(p2))
//	 * </pre>
//	 * 
//	 * @see simpledb.plan.Plan#blocksAccessed()
//	 */
//	public int blocksAccessed() {
//		return 2 * (p1.blocksAccessed() + p2.blocksAccessed()) + 1 * (p1.blocksAccessed() + p2.blocksAccessed());
//	}
//
//	/**
//	 * Return the number of records in the join. Assuming uniform distribution, the
//	 * formula is:
//	 * 
//	 * <pre>
//	 *  R(hashjoin(p1,p2)) = R(p1)*R(p2)/max{V(p1,F1),V(p2,F2)}
//	 * </pre>
//	 * 
//	 * @see simpledb.plan.Plan#recordsOutput()
//	 */
//	public int recordsOutput() {
//		int maxvals = Math.max(p1.distinctValues(fldname1), p2.distinctValues(fldname2));
//		return (p1.recordsOutput() * p2.recordsOutput()) / maxvals;
//	}
//
//	/**
//	 * Estimates the number of distinct values for the specified field.
//	 * 
//	 * @see simpledb.plan.Plan#distinctValues(java.lang.String)
//	 */
//	public int distinctValues(String fldname) {
//		if (p1.schema().hasField(fldname))
//			return p1.distinctValues(fldname);
//		else
//			return p2.distinctValues(fldname);
//	}
//
//	/**
//	 * Return the schema of the join, which is the union of the schemas of the
//	 * underlying queries.
//	 * 
//	 * @see simpledb.plan.Plan#schema()
//	 */
//	public Schema schema() {
//		return schema;
//	}
//
//	/**
//	 * Prints the plan that is being used.
//	 */
//	public void printPlan() {
//		System.out.println("Hash join plan used on " + fldname1 + opr + fldname2);
//	}
//}
