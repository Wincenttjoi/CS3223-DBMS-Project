package simpledb.opt;

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
	private Map<String, IndexInfo> indexes;
	private Transaction tx;
	private String tblname;

	/**
	 * Creates a new table planner. The specified predicate applies to the entire
	 * query. The table planner is responsible for determining which portion of the
	 * predicate is useful to the table, and when indexes are useful.
	 * 
	 * @param tblname the name of the table
	 * @param mypred  the query predicate
	 * @param tx      the calling transaction
	 */
	public TablePlanner(String tblname, Predicate mypred, Transaction tx, MetadataMgr mdm) {
		this.mypred = mypred;
		this.tx = tx;
		this.tblname = tblname;
		myplan = new TablePlan(tx, tblname, mdm);
		myschema = myplan.schema();
		indexes = mdm.getIndexInfo(tblname, tx);
	}

	/**
	 * Constructs a select plan for the table. The plan will use an indexselect, if
	 * possible.
	 * 
	 * @return a select plan for the table.
	 */
	public Plan makeSelectPlan() {
		Plan p = makeIndexSelect();
		if (p == null)
			p = myplan;
		return addSelectPred(p);
	}

	/**
	 * Constructs a join plan of the specified plan and the table. The plan will use
	 * an indexjoin, if possible. (Which means that if an indexselect is also
	 * possible, the indexjoin operator takes precedence.) The method returns null
	 * if no join is possible.
	 * 
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
			Plan nestedJoinPlan = makeProductJoin(current, currsch);
			Plan hashJoinPlan = makeHashJoin(current, currsch);

			Stream<Plan> plans = Stream.of(indexJoinPlan, mergeJoinPlan, nestedJoinPlan).filter((p1) -> p1 != null)
					.sorted((p1, p2) -> Integer.compare(p1.blocksAccessed(), p2.blocksAccessed()));
			return plans.collect(Collectors.toList()).get(0);
		}

		switch (joinAlgoSelected) {
		case INDEXJOIN_PLAN:
			p = makeIndexJoin(current, currsch);
			if (p == null) {
				p = makeProductJoin(current, currsch);
			}
			break;
		case MERGEJOIN_PLAN:
			p = makeMergeJoin(current, currsch);
			if (p == null) {
				p = makeProductJoin(current, currsch);
			}
			break;
		case NESTEDJOIN_PLAN:
			p = makeNestedJoin(current, currsch);
			break;
		case HASHJOIN_PLAN:
			p = makeHashJoin(current, currsch);
			if (p == null) {
				p = makeProductJoin(current, currsch);
			}
			break;
		default:
			throw new RuntimeException();
		}
		return p;
	}

	/**
	 * Constructs a product plan of the specified plan and this table.
	 * 
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

			// use index select if operator isn't "!=" and if idxtype is hash, operator must
			// be "="
			if (val != null && ii.supportsRangeSearch(opr)) {
				System.out.println("index select on " + fldname + opr + val);
				return new IndexSelectPlan(myplan, ii, val, opr);
			}
		}
		return null;
	}

	/**
	 * Constructs a indexjoin plan of the specified plan and this table.
	 * 
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
					System.out.println("Indexjoin blocks accessed = " + p.blocksAccessed());
					p = addSelectPred(p);
					return addJoinPred(p, currsch);
				} else {
					System.out.println(
							"Indexjoin failed: " + opr + " not supported by indexjoin, using productjoin instead");
				}
			}
		}

		System.out.println("Indexjoin failed: No index in " + tblname + " matches, using productjoin instead");
		return null;
	}

	/**
	 * Constructs a mergejoin plan of the specified plan and this table.
	 * 
	 * @param current the specified plan
	 * @return a mergejoin plan of the specified plan and this table
	 */
	private Plan makeMergeJoin(Plan current, Schema currsch) {
		Predicate joinpred = mypred.joinSubPred(currsch, myschema);
		Term joinTerm = joinpred.getTerms().get(0);
		String joinValLHS = joinTerm.getLHS().asFieldName();
		String joinValRHS = joinTerm.getRHS().asFieldName();
		boolean isCurrentPlanOnRHS = current.schema().fields().contains(joinValRHS);
		String opr = mypred.getOperatorFromFieldComparison(joinValLHS);

		Plan lhsPlan, rhsPlan;
		if (isCurrentPlanOnRHS) {
			lhsPlan = myplan;
			rhsPlan = current;
		} else {
			lhsPlan = current;
			rhsPlan = myplan;
		}

		Plan p;
		if (opr.equals(">")) {
			opr = "<";
			p = new MergeJoinPlan(tx, rhsPlan, lhsPlan, joinValRHS, joinValLHS, opr);
		} else if (opr.equals(">=")) {
			opr = "<=";
			p = new MergeJoinPlan(tx, rhsPlan, lhsPlan, joinValRHS, joinValLHS, opr);
		} else if (opr.equals("=") || opr.equals("<") || opr.equals("<=")) {
			p = new MergeJoinPlan(tx, lhsPlan, rhsPlan, joinValLHS, joinValRHS, opr);
		} else {
			System.out.println("Mergejoin failed: " + opr + " not supported by indexjoin, using productjoin instead");
			return null;
		}
		System.out.println("Mergejoin blocks accessed = " + p.blocksAccessed());
		p = addSelectPred(p);
		return addJoinPred(p, currsch);
	}

	private Plan makeProductJoin(Plan current, Schema currsch) {
		Plan p = makeProductPlan(current);
		return addJoinPred(p, currsch);
	}

	private Plan makeNestedJoin(Plan current, Schema currsch) {
		Predicate joinpred = mypred.joinSubPred(currsch, myschema);
		Term joinTerm = joinpred.getTerms().get(0);
		String joinValLHS = joinTerm.getLHS().asFieldName();
		String joinValRHS = joinTerm.getRHS().asFieldName();
		boolean isCurrentPlanOnRHS = current.schema().fields().contains(joinValRHS);
		String opr = mypred.getOperatorFromFieldComparison(joinValLHS);

		Plan lhsPlan, rhsPlan;
		if (isCurrentPlanOnRHS) {
			lhsPlan = myplan;
			rhsPlan = current;
		} else {
			lhsPlan = current;
			rhsPlan = myplan;
		}

		Plan p = new NestedJoinPlan(lhsPlan, rhsPlan, joinValLHS, joinValRHS, opr);
		p = addSelectPred(p);
		return addJoinPred(p, currsch);
	}

	/**
	 * Constructs a hashjoin plan of the specified plan and this table.
	 * 
	 * @param current the specified plan
	 * @return a hashjoin plan of the specified plan and this table
	 */
	private Plan makeHashJoin(Plan current, Schema currsch) {
		Predicate joinpred = mypred.joinSubPred(currsch, myschema);
		Term joinTerm = joinpred.getTerms().get(0);
		String joinValLHS = joinTerm.getLHS().asFieldName();
		String joinValRHS = joinTerm.getRHS().asFieldName();
		String opr = mypred.getOperatorFromFieldComparison(joinValLHS);
		boolean isCurrentPlanOnRHS = current.schema().fields().contains(joinValRHS);

		Plan lhsPlan, rhsPlan;
		if (isCurrentPlanOnRHS) {
			lhsPlan = myplan;
			rhsPlan = current;
		} else {
			lhsPlan = current;
			rhsPlan = myplan;
		}
		
		if (tx.availableBuffs() >= Math.sqrt(lhsPlan.blocksAccessed())) {
			System.out.println("Hashjoin failed: not enough buffer size available for hashjoin, using productjoin instead");
			return null;
		} else if (opr.equals("=")) {
			System.out.println("Hashjoin failed: " + opr + " not supported by hashjoin, using productjoin instead");
			return null;
		}
		int numPart = tx.availableBuffs() - 1; // max # of partitions <= B - 1
		Plan p = new HashJoinPlan(tx, lhsPlan, rhsPlan, joinValLHS, joinValRHS, opr, numPart);
		System.out.println("Hashjoin blocks accessed = " + p.blocksAccessed());
		p = addSelectPred(p);
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
