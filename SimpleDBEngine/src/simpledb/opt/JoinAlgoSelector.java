package simpledb.opt;

public enum JoinAlgoSelector {
   	INDEXJOIN_PLAN("indexjoin"),
    MERGEJOIN_PLAN("mergejoin"),
    NESTEDJOIN_PLAN("nestedjoin");
   
   	private String join_algo_name;

	JoinAlgoSelector(String join_algo_name) {
		this.join_algo_name = join_algo_name;
	}
	
	@Override
	public String toString(){
	    return join_algo_name;
	}
}