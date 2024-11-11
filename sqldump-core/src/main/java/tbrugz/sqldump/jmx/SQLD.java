package tbrugz.sqldump.jmx;

import java.sql.DatabaseMetaData;

import tbrugz.sqldump.sqlrun.jmx.SQLR;

public class SQLD extends SQLR implements SQLDMBean {
	
	public static final String MBEAN_NAME = "tbrugz.sqldump:type=SQLDump";
	
	public SQLD(int maxPosition, DatabaseMetaData dbmd) {
		super(maxPosition, dbmd);
	}

	@Override
	public String getName() {
		return "SQLDump MBean";
	}

	public void dbmdUpdate(DatabaseMetaData dbmd) {
		this.dbmd = dbmd;
	}

}
