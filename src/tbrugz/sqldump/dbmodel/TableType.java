package tbrugz.sqldump.dbmodel;

import org.apache.log4j.Logger;

public enum TableType {
	TABLE, SYNONYM, SYSTEM_TABLE,
	VIEW, MATERIALIZED_VIEW,
	EXTERNAL_TABLE;
	//XXXdone: temporary table, materialized view?
	//TODOne: external table?
	
	static Logger log = Logger.getLogger(TableType.class);
	
	public static TableType getTableType(String tableType, String tableName) {
		if(tableType.equals("TABLE")) {
			return TableType.TABLE;
		}
		else if(tableType.equals("SYNONYM")) {
			return TableType.SYNONYM;
		}
		else if(tableType.equals("VIEW")) {
			return TableType.VIEW;
		}
		else if(tableType.equals("SYSTEM TABLE")) {
			return TableType.SYSTEM_TABLE;
		}
		else if(tableType.equals("MATERIALIZED VIEW")) {
			return TableType.MATERIALIZED_VIEW;
		}
		else if(tableType.equals("EXTERNAL TABLE")) {
			return TableType.EXTERNAL_TABLE;
		}

		log.warn("table "+tableName+" of unknown type: "+tableType);
		return null;
	}
}
