package tbrugz.sqldump.dbmsfeatures;

import java.util.List;

import tbrugz.sqldump.dbmodel.DBObject;
import tbrugz.sqldump.dbmodel.Table;
import tbrugz.sqldump.util.Utils;

/*
 * TODO: property for selecting dump (true/false) of extra fields on script output?
 * XXXxxx: sys.all_external_tables, sys.all_mviews
 * 
 * see: http://download.oracle.com/docs/cd/B28359_01/server.111/b28286/statements_7002.htm
 */
public class OracleTable extends Table {
	private static final long serialVersionUID = 1L;

	public enum PartitionType {
		RANGE, HASH, LIST;
		//XXX: reference, composite-range, composite-list, system
	}
	
	static boolean dumpPhysicalAttributes = true; 
	static boolean dumpLoggingClause = true; 
	
	public String tableSpace;
	public boolean temporary;
	public boolean logging;
	
	//partition properties
	public Boolean partitioned;
	public PartitionType partitionType;
	public List<String> partitionColumns;
	public List<OracleTablePartition> partitions;
	@Deprecated //XXX?
	Integer numberOfPartitions; //only "hash_partitions_by_quantity::=", not "individual_hash_partitions::=" (number == 0)
	
	@Override
	public String getTableType4sql() {
		return temporary?"global temporary ":"";
	}
	
	@Override
	public String getTableFooter4sql() {
		String footer = "";
		if(dumpPhysicalAttributes) {
			footer += tableSpace!=null?"\ntablespace "+DBObject.getFinalIdentifier(tableSpace):"";
		}
		if(dumpLoggingClause) {
			footer += logging?"\nlogging":"";
		}
		//partition by
		if(partitioned!=null && partitioned) {
			footer += "\npartition by "+partitionType+" ("+Utils.join(partitionColumns, ", ", sqlIddecorator)+")";
			if(partitions!=null) {
				footer += " (";
				int partCount = 0;
				for(OracleTablePartition otp: partitions) {
					footer += (partCount>0?",":"")+"\n\tpartition "+otp.name;
					switch(partitionType) {
					case RANGE:
						footer += " values less than ("+Utils.join(otp.upperValues, ",")+")"; break;
					case LIST:
						footer += " values ("+Utils.join(otp.values, ",")+")"; break;
					case HASH:
					default:
						break;
					}
					footer += (dumpPhysicalAttributes?" tablespace "+otp.tableSpace:"");
					partCount++;
				}
				footer += "\n)";
			}
			else {
				if(numberOfPartitions!=null) {
					footer += "partitions "+numberOfPartitions;
				}
				else {
					throw new RuntimeException("inconsistent partition info [table = "+getName()+"]");
				}
			}
		}
		return footer; 
	}

}
