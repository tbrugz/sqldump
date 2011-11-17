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
	Integer numberOfPartitions; //only "hash_partitions_by_quantity::=", not "individual_hash_partitions::=" (number == 0)
	
	@Override
	public String getTableType4sql() {
		return temporary?"global temporary ":"";
	}
	
	@Override
	public String getTableFooter4sql() {
		String footer = tableSpace!=null?"\ntablespace "+tableSpace:"";
		footer += logging?"\nlogging":"";
		//partition by
		if(partitioned!=null && partitioned) {
			footer += "\npartition by "+partitionType+" ("+Utils.join(partitionColumns, ", ")+")";
			if(partitions!=null) {
				footer += " (\n";
				for(OracleTablePartition otp: partitions) {
					footer += "\tpartition "+otp.name+" tablespace "+otp.tableSpace+",\n";
				}
				footer += ")";
			}
			else {
				if(numberOfPartitions!=null) {
					footer += "partitions "+numberOfPartitions;
				}
				else {
					throw new RuntimeException("inconsistent partition info [table = "+name+"]");
				}
			}
		}
		return footer; 
	}

}
