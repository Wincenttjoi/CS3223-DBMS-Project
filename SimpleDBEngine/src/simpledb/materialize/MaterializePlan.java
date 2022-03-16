package simpledb.materialize;

import simpledb.tx.Transaction;
import simpledb.record.*;

import java.util.List;

import simpledb.plan.Plan;
import simpledb.plan.TablePlan;
import simpledb.query.*;

/**
 * The Plan class for the <i>materialize</i> operator.
 * @author Edward Sciore
 */
public class MaterializePlan implements Plan {
   private Plan srcplan;
   private Transaction tx;
   
   /**
    * Create a materialize plan for the specified query.
    * @param srcplan the plan of the underlying query
    * @param tx the calling transaction
    */
   public MaterializePlan(Transaction tx, Plan srcplan) {
      this.srcplan = srcplan;
      this.tx = tx;
   }
   
   /**
    * This method loops through the underlying query,
    * copying its output records into a temporary table.
    * It then returns a table scan for that table.
    * @see simpledb.plan.Plan#open()
    */
   public Scan open() {
      Schema sch = srcplan.schema();
      TempTable temp = new TempTable(tx, sch);
      Scan src = srcplan.open();
      UpdateScan dest = temp.open();
      boolean isTupleExist = false;
      while (src.next()) {
    	  isTupleExist = true;
         dest.insert();
         for (String fldname : sch.fields())
            dest.setVal(fldname, src.getVal(fldname));
      }
      if (isTupleExist) {
    	  ((TableScan) dest).setTupleExist(isTupleExist);
      }
      src.close();
      dest.beforeFirst();
      printPlan();
      return dest;
   }
   
   /**
    * Return the estimated number of blocks in the 
    * materialized table.
    * It does <i>not</i> include the one-time cost
    * of materializing the records.
    * @see simpledb.plan.Plan#blocksAccessed()
    */
   public int blocksAccessed() {
      // create a dummy Layout object to calculate record length
      Layout layout = new Layout(srcplan.schema());
      double rpb = (double) (tx.blockSize() / layout.slotSize());
      return (int) Math.ceil(srcplan.recordsOutput() / rpb);
   }
   
   /**
    * Return the number of records in the materialized table,
    * which is the same as in the underlying plan.
    * @see simpledb.plan.Plan#recordsOutput()
    */
   public int recordsOutput() {
      return srcplan.recordsOutput();
   }
   
   /**
    * Return the number of distinct field values,
    * which is the same as in the underlying plan.
    * @see simpledb.plan.Plan#distinctValues(java.lang.String)
    */
   public int distinctValues(String fldname) {
      return srcplan.distinctValues(fldname);
   }
   
   /**
    * Return the schema of the materialized table,
    * which is the same as in the underlying plan.
    * @see simpledb.plan.Plan#schema()
    */
   public Schema schema() {
      return srcplan.schema();
   }
   
   /**
    * Prints the plan that is being used.
    */
   public void printPlan() {
	   String str = "Materialized Plan created with fields [";
	   List<String> fldnames = srcplan.schema().fields();
	   for (String fldname : fldnames) {
		   str += fldname + ", ";
	   }
	   str = str.substring(0, str.length() - 2);
	   str += "]";
	   System.out.println(str);
   }
}
