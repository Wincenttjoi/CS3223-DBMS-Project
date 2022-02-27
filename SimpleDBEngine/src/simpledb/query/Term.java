package simpledb.query;

import simpledb.plan.Plan;
import simpledb.record.*;
import simpledb.materialize.OprComparator;

/**
 * A term is a comparison between two expressions.
 * @author Edward Sciore
 *
 */
public class Term {
   private Expression lhs, rhs;
   private String opr;
   
   /**
    * Create a new term that compares two expressions
    * for equality.
    * @param lhs  the LHS expression
    * @param rhs  the RHS expression
    * @param opr  the operator in relation to both expressions
    */
   public Term(Expression lhs, Expression rhs, String opr) {
      this.lhs = lhs;
      this.rhs = rhs;
      this.opr = opr;
   }
   
   /**
    * Return true if both of the term's expressions
    * satisfies the condition of the term's operator,
    * with respect to the specified scan.
    * @param s the scan
    * @return true if both expressions satisfies the condition of operator
    */
   public boolean isSatisfied(Scan s) {
      Constant lhsval = lhs.evaluate(s);
      Constant rhsval = rhs.evaluate(s);
      
      return OprComparator.compare(lhsval, rhsval, opr);
   }
   
   /**
    * Calculate the extent to which selecting on the term reduces 
    * the number of records output by a query.
    * For example if the reduction factor is 2, then the
    * term cuts the size of the output in half.
    * @param p the query's plan
    * @return the integer reduction factor.
    */
   public int reductionFactor(Plan p) {
      String lhsName, rhsName;
      if (lhs.isFieldName() && rhs.isFieldName()) {
         lhsName = lhs.asFieldName();
         rhsName = rhs.asFieldName();
         return Math.max(p.distinctValues(lhsName),
                         p.distinctValues(rhsName));
      }
      if (lhs.isFieldName()) {
         lhsName = lhs.asFieldName();
         return p.distinctValues(lhsName);
      }
      if (rhs.isFieldName()) {
         rhsName = rhs.asFieldName();
         return p.distinctValues(rhsName);
      }
      // otherwise, the term equates constants
      if (lhs.asConstant().equals(rhs.asConstant()))
         return 1;
      else
         return Integer.MAX_VALUE;
   }
   
   /**
    * Determine if this term is of the form "F opr c"
    * where F is the specified field, opr is a operator and c is some constant.
    * If so, the method returns that constant.
    * If not, the method returns null.
    * @param fldname the name of the field
    * @return either the constant or null
    */
   public Constant comparesWithConstant(String fldname) {
      if (lhs.isFieldName() &&
          lhs.asFieldName().equals(fldname) &&
          !rhs.isFieldName())
         return rhs.asConstant();
      else if (rhs.isFieldName() &&
               rhs.asFieldName().equals(fldname) &&
               !lhs.isFieldName())
         return lhs.asConstant();
      else
         return null;
   }
   
   /**
    * Retrieve the operator where this term is of the form "F opr c"
    * where F is the specified field, opr is a operator and c is some constant.
    * If so, the method returns that operator.
    * If not, the method returns null.
    * @param fldname the name of the field
    * @return either the operator or null
    */
   public String getOperator(String fldname) {
	  boolean isConstantOnLHS;
      if (lhs.isFieldName() &&
          lhs.asFieldName().equals(fldname) &&
          !rhs.isFieldName())
    	 isConstantOnLHS = false;
      else if (rhs.isFieldName() &&
               rhs.asFieldName().equals(fldname) &&
               !lhs.isFieldName())
    	  isConstantOnLHS = true;
      else
         return null;
      
      if (isConstantOnLHS) {
          switch (opr) {
		      case "<" -> { return ">="; }
		      case "<=" -> { return ">"; }
		      case ">" -> { return "<="; }
		      case ">=" -> { return "<"; }
		      default -> { return opr; }
          }
      } else {
    	  return opr;
      }
   }
   
   /**
    * Determine if this term is of the form "F1=F2"
    * where F1 is the specified field and F2 is another field.
    * If so, the method returns the name of that field.
    * If not, the method returns null.
    * @param fldname the name of the field
    * @return either the name of the other field, or null
    */
   public String equatesWithField(String fldname) {
      if (lhs.isFieldName() &&
          lhs.asFieldName().equals(fldname) &&
          rhs.isFieldName())
         return rhs.asFieldName();
      else if (rhs.isFieldName() &&
               rhs.asFieldName().equals(fldname) &&
               lhs.isFieldName())
         return lhs.asFieldName();
      else
         return null;
   }
   
   /**
    * @return LHS expression
    */
   public Expression getLHS() {
      return lhs;
   }
   
   /**
    * @return RHS expression
    */
   public Expression getRHS() {
      return rhs;
   }
   
   /**
    * Return true if both of the term's expressions
    * apply to the specified schema.
    * @param sch the schema
    * @return true if both expressions apply to the schema
    */
   public boolean appliesTo(Schema sch) {
      return lhs.appliesTo(sch) && rhs.appliesTo(sch);
   }
   
   public String toString() {
      return lhs.toString() + opr + rhs.toString();
   }
}
