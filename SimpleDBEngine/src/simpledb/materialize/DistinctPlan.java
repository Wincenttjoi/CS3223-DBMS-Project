package simpledb.materialize;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import simpledb.plan.Plan;
import simpledb.query.Scan;
import simpledb.record.Schema;

public class DistinctPlan implements Plan {
    private Plan p;
    private Schema sch = new Schema();
    private RecordComparator comp;
	   
    public DistinctPlan(Plan p, List<String> sortfields) {
    	this.p = p;
        Map<String, Boolean> sortPairs = new LinkedHashMap<>();
        for (String f : sortfields) {
    	    sortPairs.put(f, true);
        }
        comp = new RecordComparator(sortPairs);
    }
	   
	public Scan open() {
        Scan s = p.open();
        return new DistinctScan(s, comp);
	}
	
   /**
    * Estimates the number of block accesses in the projection distinct,
    * which is the same as in the underlying query.
    * @see simpledb.plan.Plan#blocksAccessed()
    */
	public int blocksAccessed() {
		return p.blocksAccessed();
	}
	
	
   /**
    * Estimates the number of output records in the projection distinct,
    * which is the same as in the underlying query.
    * @see simpledb.plan.Plan#recordsOutput()
    */
   public int recordsOutput() {
      return p.recordsOutput();
   }

   /**
    * Estimates the number of distinct field values
    * in the projection,
    * which is the same as in the underlying query.
    * @see simpledb.plan.Plan#distinctValues(java.lang.String)
    */
   public int distinctValues(String fldname) {
      return p.distinctValues(fldname);
   }
	
   /**
    * Returns the schema of the projection distinct,
    * which is taken from the field list.
    * @see simpledb.plan.Plan#schema()
    */
	public Schema schema() {
		return sch;
	}
	   
	   
}
