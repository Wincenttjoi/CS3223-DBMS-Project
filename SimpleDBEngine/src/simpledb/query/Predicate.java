package simpledb.query;

import java.util.*;
import simpledb.plan.Plan;
import simpledb.record.*;

/**
 * A predicate is a Boolean combination of terms.
 * @author Edward Sciore
 *
 */
public class Predicate {
   private List<Term> terms = new ArrayList<Term>();

   /**
    * Create an empty predicate, corresponding to "true".
    */
   public Predicate() {}

   /**
    * Create a predicate containing a single term.
    * @param t the term
    */
   public Predicate(Term t) {
      terms.add(t);
   }
   
   /**
    * Create a predicate containing a multiple term.
    * @param List<Term> list of terms
    */
   public Predicate(List<Term> t) {
	   terms.addAll(t);
   }

   /**
    * Modifies the predicate to be the conjunction of
    * itself and the specified predicate.
    * @param pred the other predicate
    */
   public void conjoinWith(Predicate pred) {
      terms.addAll(pred.terms);
   }

   /**
    * Returns true if the predicate evaluates to true
    * with respect to the specified scan.
    * @param s the scan
    * @return true if the predicate is true in the scan
    */
   public boolean isSatisfied(Scan s) {
      for (Term t : terms)
         if (!t.isSatisfied(s))
            return false;
      return true;
   }

   /** 
    * Calculate the extent to which selecting on the predicate 
    * reduces the number of records output by a query.
    * For example if the reduction factor is 2, then the
    * predicate cuts the size of the output in half.
    * @param p the query's plan
    * @return the integer reduction factor.
    */ 
   public int reductionFactor(Plan p) {
      int factor = 1;
      for (Term t : terms)
         factor *= t.reductionFactor(p);
      return factor;
   }

   /**
    * Return the subpredicate that applies to the specified schema.
    * @param sch the schema
    * @return the subpredicate applying to the schema
    */
   public Predicate selectSubPred(Schema sch) {
      Predicate result = new Predicate();
      for (Term t : terms)
         if (t.appliesTo(sch))
            result.terms.add(t);
      if (result.terms.size() == 0)
         return null;
      else
         return result;
   }

   /**
    * Return the subpredicate consisting of terms that apply
    * to the union of the two specified schemas, 
    * but not to either schema separately.
    * @param sch1 the first schema
    * @param sch2 the second schema
    * @return the subpredicate whose terms apply to the union of the two schemas but not either schema separately.
    */
   public Predicate joinSubPred(Schema sch1, Schema sch2) {
      Predicate result = new Predicate();
      Schema newsch = new Schema();
      newsch.addAll(sch1);
      newsch.addAll(sch2);
      for (Term t : terms)
         if (!t.appliesTo(sch1)  &&
               !t.appliesTo(sch2) &&
               t.appliesTo(newsch))
            result.terms.add(t);
      if (result.terms.size() == 0)
         return null;
      else
         return result;
   }

   /**
    * Determine if there is a term of the form "F opr c"
    * where F is the specified field, opr is a operator and c is some constant.
    * If so, the method returns that constant.
    * If not, the method returns null.
    * @param fldname the name of the field
    * @return either the constant or null
    */
   public Constant comparesWithConstant(String fldname) {
      for (Term t : terms) {
         Constant c = t.comparesWithConstant(fldname);
         if (c != null)
            return c;
      }
      return null;
   }
   
   /**
    * Retrieve the operator where this term is of the form "F opr c"
    * where F is the specified field, opr is a operator and c is some constant.
    * If so, the method returns that constant.
    * If not, the method returns null.
    * @param fldname the name of the field
    * @return either the constant or null
    */
   public String getOperatorFromConstantComparison(String fldname) {
      for (Term t : terms) {
         Constant c = t.comparesWithConstant(fldname);
         if (c != null)
            return t.getOperator(fldname);
      }
      return null;
   }
   
   /**
    * Retrieve the operator where this term is of the form "F opr F2"
    * where F1 is the specified field, opr is a operator and F2 is another field.
    * If so, the method returns that constant.
    * If not, the method returns null.
    * @param fldname the name of the field
    * @return either the constant or null
    */
   public String getOperatorFromFieldComparison(String fldname) {
      for (Term t : terms) {
         String s = t.comparesWithField(fldname);
         if (s != null)
            return t.getOperator(fldname);
      }
      return null;
   }
   
   /**
    * Removes term in the predicate that no longer needed
    * @param fldname
    * @return Predicate
    */
   public Predicate removeSelectField(String fldname, Constant cnst, String opr) {
	   List<Term> newTerms = new ArrayList<>();
	   for (Term t : terms) {
		   Expression lhs = t.getLHS();
		   Expression rhs = t.getRHS();
		   if (lhs.isFieldName() && !lhs.asFieldName().equals(fldname) ||
				   rhs.asConstant() != null && !rhs.asConstant().equals(cnst) ||
				   !t.getOperator(fldname).equals(opr)) {
			   newTerms.add(t);
		   }
		   
	   }
	   Predicate result = new Predicate(newTerms);
	   return result;
   }
   
   /**
    * Determine if there is a term of the form "F1 opr F2"
    * where F1 is the specified field, opr is a operator and F2 is another field.
    * If so, the method returns the name of that field.
    * If not, the method returns null.
    * @param fldname the name of the field
    * @return the name of the other field, or null
    */
   public String comparesWithField(String fldname) {
      for (Term t : terms) {
         String s = t.comparesWithField(fldname);
         if (s != null)
            return s;
      }
      return null;
   }
   
   /**
    * Return all terms in this predicate.
    * @param fldname the name of the field
    * @return the terms
    */
   public List<Term> getTerms() {
      return terms;
   }

   public String toString() {
      Iterator<Term> iter = terms.iterator();
      if (!iter.hasNext()) 
         return "";
      String result = iter.next().toString();
      while (iter.hasNext())
         result += " and " + iter.next().toString();
      return result;
   }
}
