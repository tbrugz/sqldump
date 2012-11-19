package tbrugz.sqldump.dbmsfeatures;

import java.io.Serializable;
import java.util.List;

// http://download.oracle.com/docs/cd/B28359_01/server.111/b28286/statements_7002.htm#i2125922
public class OracleTablePartition implements Serializable {
	public String name;
	public String tableSpace;

	/* range_values_clause::= */
	//TODO: remove this: keep only 'values'
	public List<String> upperValues;
	//add maxvalue?
	
	/* list_values_clause::= */
	public List<String> values;
	//add null? DEFAULT?

	/* individual_hash_partitions::= */
	//nothing, just name...
}
