package simpledb.opt;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import simpledb.tx.Transaction;
import simpledb.record.*;
import simpledb.query.*;
import simpledb.metadata.*;
import simpledb.index.planner.*;
import simpledb.materialize.MergeJoinPlan;
import simpledb.multibuffer.MultibufferProductPlan;
import simpledb.plan.*;

/**
 * This class contains methods for planning a single table.
 * @author Edward Sciore
 */
class TablePlanner {
   private TablePlan myplan;
   private Predicate mypred;
   private Schema myschema;
   private Map<String,IndexInfo> indexes;
   private Transaction tx;
   
   /**
    * Creates a new table planner.
    * The specified predicate applies to the entire query.
    * The table planner is responsible for determining
    * which portion of the predicate is useful to the table,
    * and when indexes are useful.
    * @param tblname the name of the table
    * @param mypred the query predicate
    * @param tx the calling transaction
    */
   public TablePlanner(String tblname, Predicate mypred, Transaction tx, MetadataMgr mdm) {
      this.mypred  = mypred;
      this.tx  = tx;
      myplan   = new TablePlan(tx, tblname, mdm);
      myschema = myplan.schema();
      indexes  = mdm.getIndexInfo(tblname, tx);
   }
   
   /**
    * Constructs a select plan for the table.
    * The plan will use an indexselect, if possible.
    * @return a select plan for the table.
    */
   public Plan makeSelectPlan() {
      Plan p = makeIndexSelect();
      if (p == null)
         p = myplan;
      return addSelectPred(p);
   }
   
   /**
    * Constructs a join plan of the specified plan
    * and the table.  The plan will use an indexjoin, if possible.
    * (Which means that if an indexselect is also possible,
    * the indexjoin operator takes precedence.)
    * The method returns null if no join is possible.
    * @param current the specified plan
    * @return a join plan of the plan and this table
    */
   public Plan makeJoinPlan(Plan current) {
      Schema currsch = current.schema();
      Predicate joinpred = mypred.joinSubPred(myschema, currsch);
      if (joinpred == null)
         return null;
     
     Plan indexJoinPlan = makeIndexJoin(current, currsch);
 	 Plan mergeJoinPlan = makeMergeJoin(current, currsch);
     Plan nestedJoinPlan = makeProductJoin(current, currsch);

     Stream<Plan> plans = Stream.of(indexJoinPlan, mergeJoinPlan, nestedJoinPlan)
    		 .filter((p) -> p != null)
    		 .sorted((p1, p2) -> Integer.compare(p1.blocksAccessed(), p2.blocksAccessed()))
    	     .peek(x -> System.out.println(x.toString() + ". Blocks accessed: " + x.blocksAccessed()));
     List<Plan> res = plans.collect(Collectors.toList());
     
     Plan bestPlan = res.get(0);
     return bestPlan;
   }
   
   /**
    * Constructs a product plan of the specified plan and
    * this table.
    * @param current the specified plan
    * @return a product plan of the specified plan and this table
    */
   public Plan makeProductPlan(Plan current) {
      Plan p = addSelectPred(myplan);
      return new MultibufferProductPlan(tx, current, p);
   }
   
   private Plan makeIndexSelect() {
      for (String fldname : indexes.keySet()) {
         Constant val = mypred.comparesWithConstant(fldname);
         String opr = mypred.getOperator(fldname);
         IndexInfo ii = indexes.get(fldname);
         
         // use index select if operator isn't "!=" and if idxtype is hash, operator must be "="
         if (val != null && (ii.supportsRangeSearch() || (!ii.supportsRangeSearch() && opr.equals("=")))
         	&& !opr.equals("<>") && !opr.equals("!=") ) {
            System.out.println("index select on " + fldname + opr + val);
            return new IndexSelectPlan(myplan, ii, val, opr);
         }
      }
      return null;
   }
      
   private Plan makeIndexJoin(Plan current, Schema currsch) {
      for (String fldname : indexes.keySet()) {
         String outerfield = mypred.equatesWithField(fldname);
         if (outerfield != null && currsch.hasField(outerfield)) {
            IndexInfo ii = indexes.get(fldname);
            Plan p = new IndexJoinPlan(current, myplan, ii, outerfield);
            p = addSelectPred(p);
            return addJoinPred(p, currsch);
         }
      }
      return null;
   }
   
   private Plan makeMergeJoin(Plan current, Schema currsch) {
	   Predicate joinpred = mypred.joinSubPred(currsch, myschema);
	   Term joinTerm = joinpred.getTerms().get(0);
	   String joinValLHS = joinTerm.getLHS().asFieldName();
	   String joinValRHS = joinTerm.getRHS().asFieldName();
	   if (current.schema().fields().contains(joinValRHS)) {
		   return new MergeJoinPlan(tx, current, myplan, joinValRHS, joinValLHS);
	   } else {
		   return new MergeJoinPlan(tx, current, myplan, joinValLHS, joinValRHS);
	   }
   }
   
   private Plan makeProductJoin(Plan current, Schema currsch) {
      Plan p = makeProductPlan(current);
      return addJoinPred(p, currsch);
   }
   
   private Plan addSelectPred(Plan p) {
      Predicate selectpred = mypred.selectSubPred(myschema);
      if (selectpred != null)
         return new SelectPlan(p, selectpred);
      else
         return p;
   }
   
   private Plan addJoinPred(Plan p, Schema currsch) {
      Predicate joinpred = mypred.joinSubPred(currsch, myschema);
      if (joinpred != null)
         return new SelectPlan(p, joinpred);
      else
         return p;
   }
}
