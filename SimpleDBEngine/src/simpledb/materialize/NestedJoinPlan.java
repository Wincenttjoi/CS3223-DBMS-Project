package simpledb.materialize;

import simpledb.materialize.NestedJoinScan;
import simpledb.query.Scan;
import simpledb.record.Schema;
import simpledb.plan.Plan;

/**
* The Plan class for the <i>nested join</i> operator.
  * @author Edward Sciore
  */
public class NestedJoinPlan implements Plan {
   private Plan p1, p2;
   private String fldname1, fldname2;
   private String opr;
   private Schema schema = new Schema();
   
   /**
    * Creates a new nested join node in the query tree,
    * having the two specified subqueries.
    * @param p1 the left-hand subquery
    * @param p2 the right-hand subquery
    * @param fldname1 the LHS join field
    * @param fldname2 the RHS join field
    * @param opr the relational operator between join fields
    */
   public NestedJoinPlan(Plan p1, Plan p2, String fldname1, String fldname2, String opr) {
      this.p1 = p1;
      this.p2 = p2;
      this.fldname1 = fldname1;
      this.fldname2 = fldname2;
      this.opr = opr;
	  schema.addAll(p1.schema());
      schema.addAll(p2.schema());
   }
   
   /**
    * Creates a nested join scan for this query.
    * @see simpledb.plan.Plan#open()
    */
   public Scan open() {
      Scan s1 = p1.open();
      Scan s2 = p2.open();
      printPlan();
      return new NestedJoinScan(s1, s2, fldname1, fldname2, opr);
   }
   
   /**
    * Estimates the number of block accesses in the join.
    * The formula is:
    * <pre> B(nestedjoin(p1,p2)) = B(p1) + R(p1)*B(p2) </pre>
    * @see simpledb.plan.Plan#blocksAccessed()
    */
   public int blocksAccessed() {
      return p1.blocksAccessed() + (p1.recordsOutput() * p2.blocksAccessed());
   }
   
   /**
    * Return the number of records in the join.
    * Assuming uniform distribution, the formula is:
    * <pre> R(join(p1,p2)) = R(p1)*R(p2)/max{V(p1,F1),V(p2,F2)}</pre>
    * @see simpledb.plan.Plan#recordsOutput()
    */
   public int recordsOutput() {
      int maxvals = Math.max(p1.distinctValues(fldname1),
                             p2.distinctValues(fldname2));
      return (p1.recordsOutput() * p2.recordsOutput()) / maxvals;
   }
   
   /**
    * Estimates the number of distinct values for the 
    * specified field.  
    * @see simpledb.plan.Plan#distinctValues(java.lang.String)
    */
   public int distinctValues(String fldname) {
      if (p1.schema().hasField(fldname))
         return p1.distinctValues(fldname);
      else
         return p2.distinctValues(fldname);
   }
   
   /**
    * Returns the schema of the product,
    * which is the union of the schemas of the underlying queries.
    * @see simpledb.plan.Plan#schema()
    */
   public Schema schema() {
      return schema;
   }
   
   /**
    * Prints the plan that is being used.
    */
   public void printPlan() {
	   String toStringUnqualified = this.toString().substring(this.toString().lastIndexOf(".")+1);
	   String p1String = p1.toString().substring(p1.toString().lastIndexOf(".")+1);
	   String p2String = p2.toString().substring(p2.toString().lastIndexOf(".")+1);
	   System.out.println(toStringUnqualified + " made on " + fldname1 + " of " + 
			   p1String + opr + fldname2 + " of " + p2String);
   }
}
