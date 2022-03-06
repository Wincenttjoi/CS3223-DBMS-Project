package simpledb.parse;

import java.util.*;

import simpledb.materialize.AggregationFn;
import simpledb.query.*;

/**
 * Data for the SQL <i>select</i> statement.
 * 
 * @author Edward Sciore
 */
public class QueryData {
	private boolean isDistinct;
	private List<String> fields;
	private List<AggregationFn> aggFns;
	private Collection<String> tables;
	private Predicate pred;
	private List<String> groupFields;
	private Map<String, Boolean> sortMap;

	/**
	 * Saves the field, distinct, table list and predicate.
	 */
	public QueryData(boolean isDistinct, List<String> fields, List<AggregationFn> aggFns, Collection<String> tables,
			Predicate pred, List<String> groupFields, Map<String, Boolean> sortMap) {
		this.isDistinct = isDistinct;
		this.fields = fields;
		this.aggFns = aggFns;
		this.tables = tables;
		this.pred = pred;
		this.groupFields = groupFields;
		this.sortMap = sortMap;
	}

	/**
	 * Returns the distinct mentioned in the select clause.
	 * 
	 * @return boolean whether the query is distinct
	 */
	public boolean isDistinct() {
		return isDistinct;
	}

	/**
	 * Returns the fields mentioned in the select clause.
	 * 
	 * @return a list of field names
	 */
	public List<String> fields() {
		return fields;
	}

	/**
	 * Returns the aggregated fields mentioned in the select clause.
	 * 
	 * @return a list of aggregation functions
	 */
	public List<AggregationFn> aggFns() {
		return aggFns;
	}

	/**
	 * Returns the tables mentioned in the from clause.
	 * 
	 * @return a collection of table names
	 */
	public Collection<String> tables() {
		return tables;
	}

	/**
	 * Returns the predicate that describes which records should be in the output
	 * table.
	 * 
	 * @return the query predicate
	 */
	public Predicate pred() {
		return pred;
	}

	/**
	 * Returns the fields mentioned in the group by clause.
	 * 
	 * @return a list of group field names
	 */
	public List<String> groupFields() {
		return groupFields;
	}

	/**
	 * Returns the fields mentioned in the order by clause.
	 * 
	 * @return a list of field names to be sorted on
	 */
	public Map<String, Boolean> sortMap() {
		return sortMap;
	}

	public String toString() {
		String result = "select ";
		if (isDistinct) {
			result += "distinct ";
		}
		for (String fldname : fields)
			result += fldname + ", ";
		for (AggregationFn aggFn : aggFns) {
			result += aggFn.fieldName() + ", ";
		}
		result = result.substring(0, result.length() - 2); // remove final comma
		result += " from ";
		for (String tblname : tables)
			result += tblname + ", ";
		result = result.substring(0, result.length() - 2); // remove final comma
		String predstring = pred.toString();
		if (!predstring.equals(""))
			result += " where " + predstring;
		if (!groupFields.isEmpty()) {
			result += " group by ";
			for (String groupField : groupFields)
				result += groupField + ", ";
			result = result.substring(0, result.length() - 2); // remove final comma
		}
		if (!sortMap.isEmpty()) {
			result += " order by ";
			for (Map.Entry<String, Boolean> e : sortMap.entrySet())
				result += e.getKey() + (e.getValue() ? " asc" : " desc") + ", ";
			result = result.substring(0, result.length() - 2); // remove final comma
		}
		return result;
	}
}
