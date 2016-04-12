package tbrugz.sqldump.dbmodel;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public enum TableType {
	TABLE,
	SYNONYM,
	SYSTEM_TABLE,   //h2
	VIEW,
	MATERIALIZED_VIEW,
	SYSTEM_VIEW,
	EXTERNAL_TABLE, //oracle
	BASE_TABLE,     //mysql/mariadb - https://kb.askmonty.org/en/base-table/ ?
	TYPE,           //XXX 'TYPE' table type? - postgresql
	FOREIGN_TABLE   //postgresql
	;
	
	//non-tables: INDEX, SEQUENCE, ...
	
	//XXX javadoc DatabaseMetaData#getTableTypes(): TABLE_TYPE String => table type.
	// Typical types are "TABLE", "VIEW", "SYSTEM TABLE", "GLOBAL TEMPORARY", 
	// "LOCAL TEMPORARY", "ALIAS", "SYNONYM".
	
	static final Log log = LogFactory.getLog(TableType.class);
	
	public String getName() {
		return toString().toLowerCase().replace('_', ' ');
	}

	public String getNameUpperCase() {
		return toString().toUpperCase().replace('_', ' ');
	}
	
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
		else if(tableType.equals("SYSTEM VIEW")) {
			return TableType.SYSTEM_VIEW;
		}
		else if(tableType.equals("MATERIALIZED VIEW")) {
			return TableType.MATERIALIZED_VIEW;
		}
		else if(tableType.equals("EXTERNAL TABLE")) {
			return TableType.EXTERNAL_TABLE;
		}
		else if(tableType.equals("BASE TABLE")) {
			return TableType.BASE_TABLE;
		}
		else if(tableType.equals("TYPE")) {
			//XXX: table of type TYPE?
			return TableType.TYPE;
		}
		else if(tableType.equals("FOREIGN TABLE")) {
			return TableType.FOREIGN_TABLE;
		}
		else if(tableType.equalsIgnoreCase("INDEX")) {
			log.debug("ignoring table "+tableName+" of '"+tableType+"' type");
			return null;
		}
		else if(tableType.equalsIgnoreCase("SEQUENCE")) {
			log.debug("ignoring table "+tableName+" of '"+tableType+"' type");
			return null;
		}
		else if(tableType.equalsIgnoreCase("SYSTEM INDEX")) {
			log.debug("ignoring table "+tableName+" of '"+tableType+"' type");
			return null;
		}

		log.warn("table "+tableName+" of unknown type: "+tableType+" (defaulting to TABLE)");
		return TableType.TABLE;
	}
	
	/*public boolean isPhysical() {
		switch (this) {
		case TABLE:
		case MATERIALIZED_VIEW:
			return true;
		}
		
		return false;
	}*/
	
	//XXX table-sets: TYPE? SYNONYM?
	
	public static final TableType[] COMMON_TABLE_TYPES = { TABLE, BASE_TABLE };

	public static final TableType[] PHYSICAL_TABLE_TYPES = { TABLE, BASE_TABLE, SYSTEM_TABLE, MATERIALIZED_VIEW }; //? should be 'Materialized'?
	
	public static final TableType[] VIEW_TABLE_TYPES = { VIEW, SYSTEM_VIEW, MATERIALIZED_VIEW };

	public static final TableType[] EXTERNAL_TABLE_TYPES = { EXTERNAL_TABLE, FOREIGN_TABLE };
	
	/*public static TableType[] getCommonTableTypes() {
		return COMMON_TABLE_TYPES;
	}*/
	
}
