package simpledb.materialize;

import java.util.*;
import simpledb.tx.Transaction;
import simpledb.record.*;
import simpledb.plan.Plan;
import simpledb.query.*;

/**
 * The Plan class for the <i>sort</i> operator.
 * @author Edward Sciore
 */
public class DistinctPlan implements Plan {
   private Plan p;
   private List<String> fields; 
   private RecordComparator comp;

   /**
    * Creates a new select node in the query tree,
    * having the specified subquery and predicate.
    * @param p the subquery
    * @param pred the predicate
    */
   public DistinctPlan(Plan p, List<String> fields) {
      this.p = p;
      this.fields = p.schema().fields();
      Map<String, Boolean> sortPairs = new LinkedHashMap<>();
      for (String f : fields) {
    	  sortPairs.put(f, true);
      }
      comp = new RecordComparator(sortPairs);
   }
   
   /**
    * Creates a select scan for this query.
    * @see simpledb.plan.Plan#open()
    */
   public Scan open() {
      Scan s = p.open();
	  return new DistinctScan(s, fields, comp);
   }
   
   /**
    * Estimates the number of block accesses in the selection,
    * which is the same as in the underlying query.
    * @see simpledb.plan.Plan#blocksAccessed()
    */
   public int blocksAccessed() {
      return p.blocksAccessed();
   }
   
   /**
    * Estimates the number of output records in the selection,
    * which is determined by the 
    * reduction factor of the predicate.
    * @see simpledb.plan.Plan#recordsOutput()
    */
   public int recordsOutput() {
      return p.recordsOutput();
   }
   
   /**
    * Estimates the number of distinct field values
    * in the projection.
    * If the predicate contains a term equating the specified 
    * field to a constant, then this value will be 1.
    * Otherwise, it will be the number of the distinct values
    * in the underlying query 
    * (but not more than the size of the output table).
    * @see simpledb.plan.Plan#distinctValues(java.lang.String)
    */
   public int distinctValues(String fldname) {
      return p.distinctValues(fldname);
   }
   
   /**
    * Returns the schema of the selection,
    * which is the same as in the underlying query.
    * @see simpledb.plan.Plan#schema()
    */
   public Schema schema() {
      return p.schema();
   }
   

}