package simpledb.opt;

import java.util.ArrayList;
import java.util.Arrays;
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
import simpledb.materialize.NestedJoinPlan;
import simpledb.materialize.HashJoinPlan;
import simpledb.multibuffer.MultibufferProductPlan;
import simpledb.plan.*;

/**
 * This class contains methods for planning a single table.
 * 
 * @author Edward Sciore
 */
class TablePlanner {
   private TablePlan myplan;
   private Predicate mypred;
   private Schema myschema;
   private Map<String,IndexInfo> indexes;
   private Transaction tx;
   private String tblname;
   private String tab = ">>";
   private Term[] joinTermsToRemove = new Term[4];
   private Term selectTermToRemove;

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
      this.tblname = tblname;
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
      if (p == null) {
         p = addSelectPred(myplan);
	  }  else {
		 // create a plan without the select term first, before restoring it.
		 mypred.removeTerm(selectTermToRemove);
	     p = addSelectPred(p);
	     mypred.conjoinWith(new Predicate(selectTermToRemove));
	  }
      return p;
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
   public Plan makeJoinPlan(Plan current, JoinAlgoSelector joinAlgoSelected) {
      Schema currsch = current.schema();
      Predicate joinpred = mypred.joinSubPred(myschema, currsch);
      Plan p;
      if (joinpred == null)
         return null;
            
      if (joinAlgoSelected == null) {
	      Plan indexJoinPlan = makeIndexJoin(current, currsch);
	      Plan mergeJoinPlan = makeMergeJoin(current, currsch);
	      Plan nestedJoinPlan = makeNestedJoin(current, currsch);
        Plan hashJoinPlan = makeHashJoin(current, currsch);
    	  List <Plan> plans = new ArrayList<Plan>(Arrays.asList(
    			  indexJoinPlan, mergeJoinPlan, nestedJoinPlan, hashJoinPlan));
    	  
	      Stream<Plan> plansStream = plans.stream()
	    		 .filter((p1) -> p1 != null)
	    		 .sorted((p1, p2) -> Integer.compare(p1.blocksAccessed(), p2.blocksAccessed()));
	      Plan bestPlan = plansStream.collect(Collectors.toList()).get(0);
	      Term joinTermSelectedToRemove = joinTermsToRemove[plans.indexOf(bestPlan)];
	      bestPlan = addSubpredicatesWithoutJoinTerm(bestPlan, joinTermSelectedToRemove, currsch);
	      return bestPlan;
      }
      
      switch (joinAlgoSelected) {
	      case INDEXJOIN_PLAN:
	    	  p = makeIndexJoin(current, currsch);
	    	  if (p == null)
	    		  return makeProductJoin(current, currsch);
	    	  break;
	      case MERGEJOIN_PLAN:
	    	  p = makeMergeJoin(current, currsch);
	    	  if (p == null)
	    		  return makeProductJoin(current, currsch);
	    	  break;
	      case NESTEDJOIN_PLAN:
	    	  p = makeNestedJoin(current, currsch);
	    	  break;
          case HASHJOIN_PLAN:
			  p = makeHashJoin(current, currsch);
		      if (p == null) {
		         return makeProductJoin(current, currsch);
		      }
              break;
	      default:
	    	  throw new RuntimeException();
      }
	  Term joinTermUsed = joinTermsToRemove[joinAlgoSelected.ordinal()];
	  p = addSubpredicatesWithoutJoinTerm(p, joinTermUsed, currsch);
      return p;
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
         String opr = mypred.getOperatorFromConstantComparison(fldname);
         IndexInfo ii = indexes.get(fldname);
         
         // use index select if operator isn't "!=" and if idxtype is hash, operator must be "="
         if (val != null && ii.supportsRangeSearch(opr)) {
        	selectTermToRemove = new Term(fldname, val, opr);
            return new IndexSelectPlan(myplan, ii, val, opr);
         }
      }
      return null;
   }
      
   
   /**
    * Constructs a indexjoin plan of the specified plan and
    * this table.
    * @param current the specified plan
    * @return a indexjoin plan of the specified plan and this table
    */
   private Plan makeIndexJoin(Plan current, Schema currsch) {
      for (String fldname : indexes.keySet()) {
         String outerfield = mypred.comparesWithField(fldname);
         String opr = mypred.getOperatorFromFieldComparison(fldname);
         IndexInfo ii = indexes.get(fldname);

         if (outerfield != null && currsch.hasField(outerfield)) {
        	 if (ii.supportsRangeSearch(opr)) {
	            Plan p = new IndexJoinPlan(current, myplan, ii, outerfield, opr);
	            System.out.println(tab + "Indexjoin blocks accessed = " + p.blocksAccessed());
	            joinTermsToRemove[JoinAlgoSelector.INDEXJOIN_PLAN.ordinal()] = 
	         		   new Term(new Expression(fldname), new Expression(outerfield), opr);
	            return p;
        	 } else {
                 System.out.println(tab + "Indexjoin failed: " + opr + " not supported by " + ii.getFieldName() + ", using productjoin instead");
        	 }
         }
      }
      System.out.println(tab + "Indexjoin failed: No index in " + tblname + " matches, using productjoin instead");
      return null;
   }
   
   /**
    * Constructs a mergejoin plan of the specified plan and
    * this table.
    * @param current the specified plan
    * @return a mergejoin plan of the specified plan and this table
    */
   
   private Plan makeMergeJoin(Plan current, Schema currsch) {
	   Predicate joinpred = mypred.joinSubPred(currsch, myschema);
	   Plan p;
	   for (Term joinTerm : joinpred.getTerms()) {
		   String joinValLHS = joinTerm.getLHS().asFieldName();
		   String joinValRHS = joinTerm.getRHS().asFieldName();
		   String opr = joinTerm.getOpr();
		   boolean isCurrentPlanOnRHS = current.schema().fields().contains(joinValRHS);
	
		   Plan lhsPlan, rhsPlan;
		   if (isCurrentPlanOnRHS) {
			   lhsPlan = myplan;
			   rhsPlan = current;
		   } else {
			   lhsPlan = current;
			   rhsPlan = myplan;
		   }
		   
		   if (opr.equals(">")) {
			   opr = "<";
			   p = new MergeJoinPlan(tx, rhsPlan, lhsPlan, joinValRHS, joinValLHS, opr, !isCurrentPlanOnRHS);
		   } else if (opr.equals(">=")) {
			   opr = "<=";
			   p = new MergeJoinPlan(tx, rhsPlan, lhsPlan, joinValRHS, joinValLHS, opr, !isCurrentPlanOnRHS);
		   } else if (opr.equals("=") || opr.equals("<") || opr.equals("<=")) {
			   p = new MergeJoinPlan(tx, lhsPlan, rhsPlan, joinValLHS, joinValRHS, opr, isCurrentPlanOnRHS);
		   } else {
			   System.out.println("Mergejoin: " + joinValLHS + opr + joinValRHS + " not supported");
			   continue;
		   }
	       System.out.println(tab + "Mergejoin blocks accessed = " + p.blocksAccessed());
	       joinTermsToRemove[JoinAlgoSelector.MERGEJOIN_PLAN.ordinal()] = 
	    		   new Term(new Expression(joinValLHS), new Expression(joinValRHS), joinTerm.getOpr());
		   return p;
	   }
       System.out.println(tab + "Mergejoin failed: Using productjoin instead");
	   return null;
   }
   
   private Plan makeProductJoin(Plan current, Schema currsch) {
       Plan p = makeProductPlan(current);
       p = addJoinPred(p, currsch);
       System.out.println(tab + "Productjoin blocks accessed = " + p.blocksAccessed());
       return p;
   }
   
   private Plan makeNestedJoin(Plan current, Schema currsch) {
	   Predicate joinpred = mypred.joinSubPred(currsch, myschema);
	   Term joinTerm = joinpred.getTerms().get(0);
	   Term reversedTerm = joinTerm.reverse();
	   
	   String joinValLHS = joinTerm.getLHS().asFieldName();
	   String joinValRHS = joinTerm.getRHS().asFieldName();
	   String opr = joinTerm.getOpr();
	   boolean isCurrentPlanOnRHS = current.schema().fields().contains(joinValRHS);
	  
	   if (isCurrentPlanOnRHS) {
		   joinValLHS = reversedTerm.getLHS().asFieldName();
		   joinValRHS = reversedTerm.getRHS().asFieldName();
		   opr = reversedTerm.getOpr();
	   }
	   
       Plan p = new NestedJoinPlan(current, myplan, joinValLHS, joinValRHS, opr);
       System.out.println(tab + "Nestedjoin blocks accessed = " + p.blocksAccessed());
       joinTermsToRemove[JoinAlgoSelector.NESTEDJOIN_PLAN.ordinal()] = 
    		   new Term(new Expression(joinValLHS), new Expression(joinValRHS), opr);
	   return p;
   }
  
   /**
    * Constructs a hashjoin plan of the specified plan and this table.
    * 
    * @param current the specified plan
    * @return a hashjoin plan of the specified plan and this table
    */
   private Plan makeHashJoin(Plan current, Schema currsch) {
       Predicate joinpred = mypred.joinSubPred(currsch, myschema);
	   Plan p;
	   for (Term joinTerm : joinpred.getTerms()) {
		   Term reversedTerm = joinTerm.reverse();
		   
		   String joinValLHS = joinTerm.getLHS().asFieldName();
		   String joinValRHS = joinTerm.getRHS().asFieldName();
		   String opr = joinTerm.getOpr();
		   boolean isCurrentPlanOnRHS = current.schema().fields().contains(joinValRHS);
		      
		   if (!opr.equals("=")) {
		     System.out.println("Hashjoin: " + joinValLHS + opr + joinValRHS + " not supported");
		     continue;
		   }
		   if (isCurrentPlanOnRHS) {
			   joinValLHS = reversedTerm.getLHS().asFieldName();
			   joinValRHS = reversedTerm.getRHS().asFieldName();
		   }
		      
		   int b = tx.availableBuffs();
		   int numPart = b - 1; // max # of partitions <= B - 1
		   p = new HashJoinPlan(tx, current, myplan, joinValLHS, joinValRHS, opr, numPart);
		   if (b < Math.sqrt(p.blocksAccessed())) {
		       System.out.println(
			           "Hashjoin: Not enough buffer size available for " + joinValLHS + opr + joinValRHS);
		       continue;
		   }
		   System.out.println(tab + "Hashjoin blocks accessed = " + p.blocksAccessed());
		   joinTermsToRemove[JoinAlgoSelector.HASHJOIN_PLAN.ordinal()] = 
		 	   new Term(new Expression(joinValLHS), new Expression(joinValRHS), opr);
		   return p;
	   }
       System.out.println(
	           "Hashjoin failed: using productjoin instead");
	   return null;
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
   
	/**
	 * Allows a new plan to be created without the joinTerm used by
	 * the selected join algorithm from the predicate.
	 * 
	 */
    private Plan addSubpredicatesWithoutJoinTerm(Plan p, Term joinTerm, Schema currsch) {
 	  mypred.removeTerm(joinTerm);
	  p = addSelectPred(p);
	  p = addJoinPred(p,currsch);
	  mypred.conjoinWith(new Predicate(joinTerm));
	  return p;
   }
}
