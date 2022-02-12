package simpledb.parse;

import java.util.*;

import simpledb.query.*;

/**
 * Data for the SQL <i>select</i> statement.
 * @author Edward Sciore
 */
public class QueryData {
   private List<String> fields;
   private Collection<String> tables;
   private Predicate pred;
   private Map<String,Boolean> sortPairs;
   
   /**
    * Saves the field and table list and predicate.
    */
   public QueryData(List<String> fields, Collection<String> tables, Predicate pred, Map<String,Boolean> sortPairs) {
      this.fields = fields;
      this.tables = tables;
      this.pred = pred;
      this.sortPairs = sortPairs;
   }
   
   /**
    * Returns the fields mentioned in the select clause.
    * @return a list of field names
    */
   public List<String> fields() {
      return fields;
   }
   
   /**
    * Returns the tables mentioned in the from clause.
    * @return a collection of table names
    */
   public Collection<String> tables() {
      return tables;
   }
   
   /**
    * Returns the predicate that describes which
    * records should be in the output table.
    * @return the query predicate
    */
   public Predicate pred() {
      return pred;
   }
   
   /**
    * Returns the fields mentioned in the order by clause.
    * @return a list of field names to be sorted on
    */
   public Map<String,Boolean> sortPairs() {
      return sortPairs;
   }
   
   public String toString() {
      String result = "select ";
      for (String fldname : fields)
         result += fldname + ", ";
      result = result.substring(0, result.length()-2); //remove final comma
      result += " from ";
      for (String tblname : tables)
         result += tblname + ", ";
      result = result.substring(0, result.length()-2); //remove final comma
      String predstring = pred.toString();
      if (!predstring.equals(""))
         result += " where " + predstring;
      if (!sortPairs.isEmpty())
    	  result += " order by ";
      	  for (Map.Entry<String, Boolean> pair : sortPairs.entrySet())
      		  result += pair.getKey() + (pair.getValue() ? " asc " : " desc ") + ", ";
	      result = result.substring(0, result.length()-2); //remove final comma
      return result;
   }
}
