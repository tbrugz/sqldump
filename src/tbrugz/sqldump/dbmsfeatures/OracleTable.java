package tbrugz.sqldump.dbmsfeatures;

import java.util.List;

import tbrugz.sqldump.Utils;
import tbrugz.sqldump.dbmodel.Table;

/*
 * TODO: property for selecting dump (true/false) of extra fields on script output?
 * XXXxxx: sys.all_external_tables, sys.all_mviews
 */
public class OracleTable extends Table {
	
	public enum PartitionType {
		RANGE, HASH, LIST;
		//XXX: reference, composite-range, composite-list, system
	}
	
	public String tableSpace;
	public boolean temporary;
	public boolean logging;
	
	//partition properties
	public Boolean partitioned;
	public PartitionType partitionType;
	public List<String> partitionColumns;
	public List<OracleTablePartition> partitions;
	int numberOfHashPartitions; //only "hash_partitions_by_quantity::=", not "individual_hash_partitions::=" (number == 0)
	
	@Override
	public String getTableType4sql() {
		return temporary?"global temporary ":"";
	}
	
	@Override
	public String getTableFooter4sql() {
		String footer = tableSpace!=null?"\nTABLESPACE "+tableSpace:"";
		footer += logging?"\nLOGGING":"";
		//partition by
		if(partitioned!=null && partitioned) {
			footer += "\nPARTITION BY "+partitionType+" ("+Utils.join(partitionColumns, ", ")+")";
		}
		return footer; 
	}

}
