package tbrugz.sqldump.dbmodel;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public enum TableType {
	TABLE, SYNONYM, SYSTEM_TABLE,
	VIEW, MATERIALIZED_VIEW,
	EXTERNAL_TABLE;
	//XXXdone: temporary table, materialized view?
	//TODOne: external table?
	
	static Log log = LogFactory.getLog(TableType.class);
	
	public static TableType getTableType(String tableType, String tableName) {
		if(tableType.equals("TABLE")) {
			return TableType.TABLE;
		}
		else if(tableType.equals("SYNONYM")) {
			//XXX: synonym of what?
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
		else if(tableType.equalsIgnoreCase("INDEX")) {
			log.debug("ignoring table "+tableName+" of '"+tableType+"' type");
			return null;
		}

		log.warn("table "+tableName+" of unknown type: "+tableType);
		return null;
	}
}
