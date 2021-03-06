package simpledb.materialize;

import simpledb.tx.Transaction;
import simpledb.plan.Plan;
import simpledb.query.*;
import simpledb.record.*;

import java.util.*;

/**
 * The Plan class for the <i>mergejoin</i> operator.
 * @author Edward Sciore
 */
public class MergeJoinPlan implements Plan {
   private Plan p1, p2;
   private String fldname1, fldname2;
   private Schema sch = new Schema();
   private String opr;
   private boolean isCurrentPlanOnRHS;
   
   /**
    * Creates a mergejoin plan for the two specified queries.
    * The RHS must be materialized after it is sorted, 
    * in order to deal with possible duplicates.
    * @param p1 the LHS query plan
    * @param p2 the RHS query plan
    * @param fldname1 the LHS join field
    * @param fldname2 the RHS join field
    * @param opr the relational operator between join fields
    * @param tx the calling transaction
    */
   public MergeJoinPlan(Transaction tx, Plan p1, Plan p2, String fldname1, String fldname2, String opr, boolean isCurrentPlanOnRHS) {
      this.fldname1 = fldname1;
      List<String> sortlist1 = Arrays.asList(fldname1);
      this.p1 = new SortPlan(tx, p1, sortlist1);
      
      this.fldname2 = fldname2;
      List<String> sortlist2 = Arrays.asList(fldname2);
      this.p2 = new SortPlan(tx, p2, sortlist2);
      
      this.opr = opr;
      sch.addAll(p1.schema());
      sch.addAll(p2.schema());
      this.isCurrentPlanOnRHS = isCurrentPlanOnRHS;
   }
   
   /** The method first sorts its two underlying scans
     * on their join field. It then returns a mergejoin scan
     * of the two sorted table scans.
     * @see simpledb.plan.Plan#open()
     */
   public Scan open() {
	  Scan s1;
	  SortScan s2;
	  if (isCurrentPlanOnRHS) {
		  s2 = (SortScan) p2.open();
		  s1 = p1.open();
	  } else {
		  s1 = p1.open();
		  s2 = (SortScan) p2.open();
	  }
      printPlan();
      return new MergeJoinScan(s1, s2, fldname1, fldname2, opr);
   }

   /**
    * Return the number of block accesses required to
    * mergejoin the sorted tables.
    * It includes the one-time cost
    * of materializing and sorting the records.
    * The formula is:
    * <pre> B(mergejoin(p1,p2,fldname1,fldname2)) = B(sort(p1, fldname1)) 
    *       + B(sort(p2, fldname2)) + B(p1) + B(p2) </pre>
    * We assume the best case scenario for merging
    * @see simpledb.plan.Plan#blocksAccessed()
    */
   public int blocksAccessed() {
	  SortPlan p1 = (SortPlan) this.p1;
	  SortPlan p2 = (SortPlan) this.p2;
      return p1.blocksAccessedWithSortCost() + p2.blocksAccessedWithSortCost() + 
    		  this.p1.blocksAccessed() + this.p2.blocksAccessed();
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
    * Return the schema of the join,
    * which is the union of the schemas of the underlying queries.
    * @see simpledb.plan.Plan#schema()
    */
   public Schema schema() {
      return sch;
   }
   
   public String toString() {
	  return "Merge join plan used on " + fldname1 + opr + fldname2;
   }
   
   /**
    * Prints the plan that is being used.
    */
   public void printPlan() {
	   System.out.println("Merge join plan used on " + fldname1 + opr + fldname2);
   }
}

