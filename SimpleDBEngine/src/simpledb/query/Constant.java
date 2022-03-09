package simpledb.query;

/**
 * The class that denotes values stored in the database.
 * @author Edward Sciore
 */
public class Constant implements Comparable<Constant> {
   private Integer ival = null;
   private Float fval = null;
   private String  sval = null;
   
   public Constant(Integer ival) {
      this.ival = ival;
   }
   
   public Constant(Float fval) {
      this.fval = fval;
   }
   
   public Constant(String sval) {
      this.sval = sval;
   }
   
   public int asInt() {
      return ival;
   }
   
   public float asFloat() {
	   return fval;
   }
   
   public String asString() {
      return sval;
   }
   
   public boolean equals(Object obj) {
      Constant c = (Constant) obj;
      if (c == null) {
    	  return false;
      }
      return (ival != null) ? ival.equals(c.ival) : sval.equals(c.sval);
   }
   
   public int compareTo(Constant c) {
      return (ival != null) ? ival.compareTo(c.ival) : sval.compareTo(c.sval);
   }
   
   public int hashCode() {
      return (ival != null) ? ival.hashCode() : sval.hashCode();
   }
   
   public String toString() {
      return (ival != null) ? ival.toString() : sval.toString();
   }   
}
