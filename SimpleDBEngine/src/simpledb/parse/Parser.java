package simpledb.parse;

import java.util.*;

import simpledb.query.*;
import simpledb.record.*;
import simpledb.opt.JoinAlgoSelector;

/**
 * The SimpleDB parser.
 * @author Edward Sciore
 */
public class Parser {
   private Lexer lex;
   private Predicate currPred;
   
   public Parser(String s) {
      lex = new Lexer(s);
      currPred = new Predicate();
   }
   
// Methods for parsing predicates, terms, expressions, constants, and fields
   
   public String field() {
      return lex.eatId();
   }
   
   public Constant constant() {
      if (lex.matchStringConstant())
         return new Constant(lex.eatStringConstant());
      else
         return new Constant(lex.eatIntConstant());
   }
   
   public Expression expression() {
      if (lex.matchId())
         return new Expression(field());
      else
         return new Expression(constant());
   }
   
   public Term term() {
      Expression lhs = expression();
      String opr = lex.eatOpr();
      Expression rhs = expression();
      return new Term(lhs, rhs, opr);
   }
   
   public Predicate predicate() {
      Predicate pred = new Predicate(term());
      if (lex.matchKeyword("and")) {
         lex.eatKeyword("and");
         pred.conjoinWith(predicate());
      }
      return pred;
   }
   
// Methods for parsing queries
   
   public QueryData query() {
      JoinAlgoSelector joinAlgoSelected = null;
      for (JoinAlgoSelector selector : JoinAlgoSelector.values()) {
    	  if (lex.matchKeyword(selector.toString())) {
              lex.eatKeyword(selector.toString());
              joinAlgoSelected = selector;
              break;
    	  }
      }
	   
      lex.eatKeyword("select");
      boolean isDistinct = false;
      if (lex.matchKeyword("distinct")) {
    	  isDistinct = true;
    	  lex.eatKeyword("distinct");
      }
      List<String> fields = selectList();
      lex.eatKeyword("from");
      Predicate pred = new Predicate();
      
      Collection<String> tables = tableList();
      pred.conjoinWith(currPred);
      currPred = new Predicate();
      
      Map<String,Boolean> sortMap = new LinkedHashMap<>();
      if (lex.matchKeyword("where")) {
         lex.eatKeyword("where");
         pred.conjoinWith(predicate());
      }

      if (lex.matchKeyword("order")) {
          lex.eatKeyword("order");
          lex.eatKeyword("by");
          sortMap = sortList();
       }
      
      return new QueryData(fields, isDistinct, tables, pred, sortMap, joinAlgoSelected);
   }
   
   private List<String> selectList() {
      List<String> L = new ArrayList<String>();
      L.add(field());
      if (lex.matchDelim(',')) {
         lex.eatDelim(',');
         L.addAll(selectList());
      }
      return L;
   }
   
   private Collection<String> tableList() {
      Collection<String> L = new ArrayList<String>();
      L.add(lex.eatId());
      
      while (lex.matchKeyword("join")) {
          lex.eatKeyword("join");
    	  L.add(lex.eatId());
    	  lex.eatKeyword("on");
    	  currPred.conjoinWith(predicate());
       }
      
      if (lex.matchDelim(',')) {
         lex.eatDelim(',');
         L.addAll(tableList());
      }
      return L;
   }
   
   private Map<String,Boolean> sortList() {
	  // parse order by clause
	  Map<String,Boolean> M = new LinkedHashMap<>(); 
	  String sField = field();
	  Boolean sType = true; // sort type is ascending by default
      if (lex.matchSortType()) {
    	  sType = lex.eatSortType();
      } else if (!lex.matchDelim(',') && !lex.matchEnd()) {
    	  throw new BadSyntaxException();
      }
      M.put(sField, sType);
      if (lex.matchDelim(',')) {
         lex.eatDelim(',');
         M.putAll(sortList());
      }
      return M;
   }
   
// Methods for parsing the various update commands
   
   public Object updateCmd() {
      if (lex.matchKeyword("insert"))
         return insert();
      else if (lex.matchKeyword("delete"))
         return delete();
      else if (lex.matchKeyword("update"))
         return modify();
      else
         return create();
   }
   
   private Object create() {
      lex.eatKeyword("create");
      if (lex.matchKeyword("table"))
         return createTable();
      else if (lex.matchKeyword("view"))
         return createView();
      else
         return createIndex();
   }
   
// Method for parsing delete commands
   
   public DeleteData delete() {
      lex.eatKeyword("delete");
      lex.eatKeyword("from");
      String tblname = lex.eatId();
      Predicate pred = new Predicate();
      if (lex.matchKeyword("where")) {
         lex.eatKeyword("where");
         pred = predicate();
      }
      return new DeleteData(tblname, pred);
   }
   
// Methods for parsing insert commands
   
   public InsertData insert() {
      lex.eatKeyword("insert");
      lex.eatKeyword("into");
      String tblname = lex.eatId();
      lex.eatDelim('(');
      List<String> flds = fieldList();
      lex.eatDelim(')');
      lex.eatKeyword("values");
      lex.eatDelim('(');
      List<Constant> vals = constList();
      lex.eatDelim(')');
      return new InsertData(tblname, flds, vals);
   }
   
   private List<String> fieldList() {
      List<String> L = new ArrayList<String>();
      L.add(field());
      if (lex.matchDelim(',')) {
         lex.eatDelim(',');
         L.addAll(fieldList());
      }
      return L;
   }
   
   private List<Constant> constList() {
      List<Constant> L = new ArrayList<Constant>();
      L.add(constant());
      if (lex.matchDelim(',')) {
         lex.eatDelim(',');
         L.addAll(constList());
      }
      return L;
   }
   
// Method for parsing modify commands
   
   public ModifyData modify() {
      lex.eatKeyword("update");
      String tblname = lex.eatId();
      lex.eatKeyword("set");
      String fldname = field();
      lex.eatDelim('=');
      Expression newval = expression();
      Predicate pred = new Predicate();
      if (lex.matchKeyword("where")) {
         lex.eatKeyword("where");
         pred = predicate();
      }
      return new ModifyData(tblname, fldname, newval, pred);
   }
   
// Method for parsing create table commands
   
   public CreateTableData createTable() {
      lex.eatKeyword("table");
      String tblname = lex.eatId();
      lex.eatDelim('(');
      Schema sch = fieldDefs();
      lex.eatDelim(')');
      return new CreateTableData(tblname, sch);
   }
   
   private Schema fieldDefs() {
      Schema schema = fieldDef();
      if (lex.matchDelim(',')) {
         lex.eatDelim(',');
         Schema schema2 = fieldDefs();
         schema.addAll(schema2);
      }
      return schema;
   }
   
   private Schema fieldDef() {
      String fldname = field();
      return fieldType(fldname);
   }
   
   private Schema fieldType(String fldname) {
      Schema schema = new Schema();
      if (lex.matchKeyword("int")) {
         lex.eatKeyword("int");
         schema.addIntField(fldname);
      }
      else {
         lex.eatKeyword("varchar");
         lex.eatDelim('(');
         int strLen = lex.eatIntConstant();
         lex.eatDelim(')');
         schema.addStringField(fldname, strLen);
      }
      return schema;
   }
   
// Method for parsing create view commands
   
   public CreateViewData createView() {
      lex.eatKeyword("view");
      String viewname = lex.eatId();
      lex.eatKeyword("as");
      QueryData qd = query();
      return new CreateViewData(viewname, qd);
   }
   
   
//  Method for parsing create index commands
   
   public CreateIndexData createIndex() {
      lex.eatKeyword("index");
      String idxname = lex.eatId();
      lex.eatKeyword("on");
      String tblname = lex.eatId();
      lex.eatDelim('(');
      String fldname = field();
      lex.eatDelim(')');
      lex.eatKeyword("using");
      String idxtype = lex.eatIdxType();
      return new CreateIndexData(idxname, idxtype, tblname, fldname);
   }
}

