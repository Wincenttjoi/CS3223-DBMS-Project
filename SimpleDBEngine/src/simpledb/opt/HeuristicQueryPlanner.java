package simpledb.opt;

import java.util.*;
import simpledb.tx.Transaction;
import simpledb.metadata.MetadataMgr;
import simpledb.parse.QueryData;
import simpledb.plan.*;
import simpledb.materialize.*;

/**
 * A query planner that optimizes using a heuristic-based algorithm.
 * 
 * @author Edward Sciore
 */
public class HeuristicQueryPlanner implements QueryPlanner {
	private Collection<TablePlanner> tableplanners = new ArrayList<>();
	private MetadataMgr mdm;

	public HeuristicQueryPlanner(MetadataMgr mdm) {
		this.mdm = mdm;
	}

	/**
	 * Creates an optimized left-deep query plan using the following heuristics. H1.
	 * Choose the smallest table (considering selection predicates) to be first in
	 * the join order. H2. Add the table to the join order which results in the
	 * smallest output.
	 */
	public Plan createPlan(QueryData data, Transaction tx) {

		// for debugging
		// System.out.println(data.toString());

		// Step 1: Create a TablePlanner object for each mentioned table
		for (String tblname : data.tables()) {
			TablePlanner tp = new TablePlanner(tblname, data.pred(), tx, mdm);
			tableplanners.add(tp);
		}

		// Step 2: Choose the lowest-size plan to begin the join order
		Plan currentplan = getLowestSelectPlan();

		// Step 3: Repeatedly add a plan to the join order
		while (!tableplanners.isEmpty()) {
			Plan p = getLowestJoinPlan(currentplan, data.joinAlgoSelected());
			if (p != null)
				currentplan = p;
			else // no applicable join
				currentplan = getLowestProductPlan(currentplan);
		}

		// Step 4. Project on the field names and return
		Plan projectplan = new ProjectPlan(currentplan, data.fields());

		// Step 5. Aggregate results on group fields and return (Lab5)
		if (!data.aggFns().isEmpty() || !data.groupFields().isEmpty()) {
			projectplan = new GroupByPlan(tx, projectplan, data.groupFields(), data.aggFns());
		}

		// Step 6. Eliminate duplicate results and return (Lab6)
		if (data.isDistinct()) {
			projectplan = new SortPlan(tx, projectplan, data.fields());
			projectplan = new DistinctPlan(projectplan);
		}

		// Step 7. Sort results on sort fields and return (Lab3)
		if (!data.sortMap().isEmpty()) {
			projectplan = new SortPlan(tx, projectplan, data.sortMap());
		}

		return projectplan;
	}

	private Plan getLowestSelectPlan() {
		TablePlanner besttp = null;
		Plan bestplan = null;
		for (TablePlanner tp : tableplanners) {
			Plan plan = tp.makeSelectPlan();
			if (bestplan == null || plan.recordsOutput() < bestplan.recordsOutput()) {
				besttp = tp;
				bestplan = plan;
			}
		}
		tableplanners.remove(besttp);
		return bestplan;
	}

	private Plan getLowestJoinPlan(Plan current, JoinAlgoSelector joinAlgoSelected) {
		TablePlanner besttp = null;
		Plan bestplan = null;
		for (TablePlanner tp : tableplanners) {
			Plan plan = tp.makeJoinPlan(current, joinAlgoSelected);
			if (plan != null && (bestplan == null || plan.recordsOutput() < bestplan.recordsOutput())) {
				besttp = tp;
				bestplan = plan;
			}
		}
		if (bestplan != null) {
			tableplanners.remove(besttp);
		}
		return bestplan;
	}

	private Plan getLowestProductPlan(Plan current) {
		TablePlanner besttp = null;
		Plan bestplan = null;
		for (TablePlanner tp : tableplanners) {
			Plan plan = tp.makeProductPlan(current);
			if (bestplan == null || plan.recordsOutput() < bestplan.recordsOutput()) {
				besttp = tp;
				bestplan = plan;
			}
		}
		tableplanners.remove(besttp);
		return bestplan;
	}

	public void setPlanner(Planner p) {
		// for use in planning views, which
		// for simplicity this code doesn't do.
	}

}
