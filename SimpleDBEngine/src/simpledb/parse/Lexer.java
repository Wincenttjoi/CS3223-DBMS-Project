package simpledb.parse;

import java.util.*;
import java.io.*;

/**
 * The lexical analyzer.
 * @author Edward Sciore
 */
public class Lexer {
   private Collection<String> keywords;
   private Collection<Character> equality_keywords;
   private Collection<String> idxType;
   private Collection<String> sortType;
   private StreamTokenizer tok;
   
   /**
    * Creates a new lexical analyzer for SQL statement s.
    * @param s the SQL statement
    */
   public Lexer(String s) {
      initKeywords();
      initOperators();
      initIdxType();
      initSortType();
      tok = new StreamTokenizer(new StringReader(s));
      tok.ordinaryChar('.');   //disallow "." in identifiers
      tok.wordChars('_', '_'); //allow "_" in identifiers
      tok.lowerCaseMode(true); //ids and keywords are converted
      nextToken();
   }
   
//Methods to check the status of the current token
   
   /**
    * Returns true if the current token is
    * the specified delimiter character.
    * @param d a character denoting the delimiter
    * @return true if the delimiter is the current token
    */
   public boolean matchDelim(char d) {
      return d == (char)tok.ttype;
   }
   
   /**
    * Returns true if the current token is an integer.
    * @return true if the current token is an integer
    */
   public boolean matchIntConstant() {
      return tok.ttype == StreamTokenizer.TT_NUMBER;
   }
   
   /**
    * Returns true if the current token is a string.
    * @return true if the current token is a string
    */
   public boolean matchStringConstant() {
      return '\'' == (char)tok.ttype;
   }
   
   /**
    * Returns true if the current token is the specified keyword.
    * @param w the keyword string
    * @return true if that keyword is the current token
    */
   public boolean matchKeyword(String w) {
      return tok.ttype == StreamTokenizer.TT_WORD && keywords.contains(w) &&
    		  tok.sval.equals(w);
   }
   
   /**
    * Returns true if the current token is a legal identifier.
    * @return true if the current token is an identifier
    */
   public boolean matchId() {
      return  tok.ttype==StreamTokenizer.TT_WORD && 
    		  !keywords.contains(tok.sval);
   }
   
   /**
    * Returns true if the current token is the specified keyword.
    * @param w the keyword string
    * @return true if that keyword is the current token
    */
   public boolean matchOpr(char c) {
      return equality_keywords.contains(c);
   }
   
   /**
    * Returns true if the current token is the specified keyword for indexing type.
    * @param w the keyword string
    * @return true if that keyword is the current token
    */
   public boolean matchIdxType(String w) {
      return idxType.contains(w);
   }
   
   /**
    * Returns true if the current token is a legal sort type.
    * @return true if the current token is a sort type
    */
   public boolean matchSortType() {
      return  tok.ttype==StreamTokenizer.TT_WORD && sortType.contains(tok.sval);
   }
   
   /**
    * Returns true if the current token is a legal terminal token.
    * @return true if the current token is a terminal token
    */
   public boolean matchEnd() {
      return  tok.ttype==StreamTokenizer.TT_EOF;
   }
   
//Methods to "eat" the current token
   
   /**
    * Throws an exception if the current token is not the
    * specified delimiter. 
    * Otherwise, moves to the next token.
    * @param d a character denoting the delimiter
    */
   public void eatDelim(char d) {
      if (!matchDelim(d))
         throw new BadSyntaxException();
      nextToken();
   }
   
   /**
    * Throws an exception if the current token is not 
    * an integer. 
    * Otherwise, returns that integer and moves to the next token.
    * @return the integer value of the current token
    */
   public int eatIntConstant() {
      if (!matchIntConstant())
         throw new BadSyntaxException();
      int i = (int) tok.nval;
      nextToken();
      return i;
   }
   
   /**
    * Throws an exception if the current token is not 
    * a string. 
    * Otherwise, returns that string and moves to the next token.
    * @return the string value of the current token
    */
   public String eatStringConstant() {
      if (!matchStringConstant())
         throw new BadSyntaxException();
      String s = tok.sval; //constants are not converted to lower case
      nextToken();
      return s;
   }
   
   /**
    * Throws an exception if the current token is not the
    * specified keyword. 
    * Otherwise, moves to the next token.
    * @param w the keyword string
    */
   public void eatKeyword(String w) {
      if (!matchKeyword(w))
         throw new BadSyntaxException();
      nextToken();
   }
   
   /**
    * Throws an exception if the current token is not 
    * an identifier. 
    * Otherwise, returns the identifier string 
    * and moves to the next token.
    * @return the string value of the current token
    */
   public String eatId() {
      if (!matchId())
         throw new BadSyntaxException();
      String s = tok.sval;
      nextToken();
      return s;
   }
   
   /**
    * Throws an exception if the current token is not the
    * specified equality keyword. 
    * Otherwise, moves to the next token.
    * @param w the keyword string
    * @return the string value of the equality token
    */
   public String eatOpr() {
	  String opr = "";
	  char c1 = (char) tok.ttype;
	  if (matchOpr(c1)) {
		  opr += c1;
	  }
	  nextToken();
	  char c2 = (char) tok.ttype;
	  if (matchOpr(c2)) {
		  opr += c2;
		  nextToken();
	  }
	  validate_equality(opr);
	  return opr;
   }
   
   /**
    * Throws an exception if the current token is not the
    * specified index type. 
    * Otherwise, moves to the next token.
    * @param w the keyword string
    * @return the string value of the equality token
    */
   public String eatIdxType() {
       String s = tok.sval;
       if (!matchIdxType(s))
    	   throw new BadSyntaxException();
       nextToken();
       return s;
   }
   
   /**
    * Throws an exception if the current token is not 
    * a sort type. 
    * Otherwise, returns the sort type boolean 
    * and moves to the next token.
    * @return the string value of the current token
    */
   public Boolean eatSortType() {
      if (!matchSortType())
         throw new BadSyntaxException();
      String s = tok.sval;
      nextToken();
      return s.equals("asc");
   }
   
   private void nextToken() {
      try {
         tok.nextToken();
      }
      catch(IOException e) {
         throw new BadSyntaxException();
      }
   }
   
   private void initKeywords() {
      keywords = Arrays.asList("select", "from", "where", "and",
                               "insert", "into", "values", "delete", "update", "set", 
                               "create", "table", "int", "varchar", "view", "as", "index", "using", "on",
                               "order", "by", "distinct", "indexjoin", "mergejoin", "nestedjoin");
   }
   
   private void initOperators() {
	   equality_keywords = Arrays.asList('=', '>', '<', '!');
   }
   
   private void initIdxType() {
	   idxType = Arrays.asList("hash", "btree");
   }
   
   private void initSortType() {
	   sortType = Arrays.asList("asc", "desc");
   }
   
   private void validate_equality(String opr) {
	   Collection<String> is_equality = Arrays.asList("=", "<", ">", ">=", "<=", "!=", "<>");
	   if (!is_equality.contains(opr)) {
		   throw new BadSyntaxException();
	   }
   }
}