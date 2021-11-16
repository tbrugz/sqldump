package tbrugz.sqldump.dbmsfeatures;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import tbrugz.sqldump.dbmodel.DBObject;
import tbrugz.sqldump.dbmodel.NamedDBObject;
import tbrugz.sqldump.dbmodel.PartitionType;
import tbrugz.sqldump.dbmodel.Table;
import tbrugz.sqldump.dbmodel.TableType;
import tbrugz.sqldump.util.Utils;

public class PostgreSqlTable extends Table {

	private static final long serialVersionUID = 1L;

	// foreign table properties
	String foreignTableServer;
	Map<String,String> foreignTableOptions;
	
	// partitioned table properties
	PartitionType partitionType;
	List<String> partitionColumns;

	// table partition properties
	NamedDBObject baseTable;
	String partitionExpression;
	
	@Override
	public String getTableType4sql() {
		if(getType()==TableType.FOREIGN_TABLE) { return "foreign"; }
		return super.getTableType4sql();
	}
	
	@Override
	public String getTableFooter4sql(boolean dumpWithSchemaName) {
		if(getType()==TableType.FOREIGN_TABLE) {
			StringBuilder sb = new StringBuilder();
			sb.append("\nserver ").append(foreignTableServer).append("\n");
			sb.append("options (");
			List<String> options = new ArrayList<String>();
			for(Map.Entry<String, String> e: foreignTableOptions.entrySet()) {
				options.add(e.getKey()+" '"+e.getValue()+"'");
			}
			sb.append(Utils.join(options, ", ")).append(")");
			return sb.toString();
		}
		if(getType()==TableType.PARTITIONED_TABLE) {
			StringBuilder sb = new StringBuilder();
			sb.append("\npartition by "+partitionType+" ("+Utils.join(partitionColumns, ", ")+")\n");
			return sb.toString();
		}
		if(getType()==TableType.TABLE_PARTITION) {
			return "\npartition of "+DBObject.getFinalName(baseTable, dumpWithSchemaName)+" "+partitionExpression+"\n";
		}
		return super.getTableFooter4sql(dumpWithSchemaName);
	}
	
	@Override
	public boolean shouldDumpInnerElements() {
		return getType()!=TableType.TABLE_PARTITION;
	}

	
	// ---
	
	public String getForeignTableServer() {
		return foreignTableServer;
	}
	public void setForeignTableServer(String foreignTableServer) {
		this.foreignTableServer = foreignTableServer;
	}
	public Map<String, String> getForeignTableOptions() {
		return foreignTableOptions;
	}
	public void setForeignTableOptions(Map<String, String> foreignTableOptions) {
		this.foreignTableOptions = foreignTableOptions;
	}
}
