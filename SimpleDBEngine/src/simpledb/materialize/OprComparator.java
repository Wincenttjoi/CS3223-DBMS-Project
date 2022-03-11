package simpledb.materialize;

import simpledb.query.*;

/**
 * A comparator for constants.
 * @author Edward Sciore
 */
public class OprComparator {
   
   /**
    * Compare two specified constants.
    *  
    * @param s1 the first Constant
    * @param s2 the second Constant
    * @param opr the relational operator to compare s1 and s2 with
    * @return the boolean result of evaluation
    */
   public static boolean compare(Constant s1, Constant s2, String opr) {
 	  boolean isLhsSmaller = s1.compareTo(s2) == -1;
      boolean isEqual = s1.equals(s2);
	      
      switch (opr) {
          case "=" -> { return isEqual; }
          case "<" -> { return isLhsSmaller; }
          case "<=" -> { return isLhsSmaller || isEqual; }
          case ">" -> { return !isLhsSmaller && !isEqual; }

          case ">=" -> { return !isLhsSmaller || isEqual; }
          case "!=", "<>" -> { return !isEqual; }
          default -> { return false; }
      }
   }
}
