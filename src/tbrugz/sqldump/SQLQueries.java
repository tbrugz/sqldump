package tbrugz.sqldump;

import java.sql.Connection;
import java.util.Properties;

import org.apache.log4j.Logger;

public class SQLQueries {

	static Logger log = Logger.getLogger(SQLQueries.class);
	
	public static void doQueries(Connection conn, Properties prop) {
		DataDump dd = new DataDump();
		
		Long globalRowLimit = Utils.getPropLong(prop, DataDump.PROP_DATADUMP_ROWLIMIT);
		String recordDelimiter = prop.getProperty(DataDump.PROP_DATADUMP_RECORDDELIMITER, DataDump.DELIM_RECORD_DEFAULT);
		String columnDelimiter = prop.getProperty(DataDump.PROP_DATADUMP_COLUMNDELIMITER, DataDump.DELIM_COLUMN_DEFAULT);
		String charset = prop.getProperty(DataDump.PROP_DATADUMP_CHARSET, DataDump.CHARSET_DEFAULT);
		boolean doTableNameHeaderDump = "true".equals(prop.getProperty(DataDump.PROP_DATADUMP_TABLENAMEHEADER, "false"));
		boolean doColumnNamesHeaderDump = "true".equals(prop.getProperty(DataDump.PROP_DATADUMP_COLUMNNAMESHEADER, "false"));
		boolean doColumnNamesDump = "true".equals(prop.getProperty(DataDump.PROP_DATADUMP_INSERTINTO_WITHCOLNAMES, "true"));
		boolean dumpInsertInfoSyntax = false, dumpCSVSyntax = false, dumpJSONSyntax = false;
		String syntaxes = prop.getProperty(DataDump.PROP_DATADUMP_SYNTAXES);
		if(syntaxes==null) {
			log.warn("no datadump syntax defined");
			return;
		}
		String[] syntaxArr = syntaxes.split(",");
		for(String syntax: syntaxArr) {
			if(DataDump.SYNTAX_INSERTINTO.equals(syntax.trim())) {
				dumpInsertInfoSyntax = true;
			}
			else if(DataDump.SYNTAX_CSV.equals(syntax.trim())) {
				dumpCSVSyntax = true;
			}
			else if(DataDump.SYNTAX_JSON.equals(syntax.trim())) {
				dumpJSONSyntax = true;
			}
			else {
				log.warn("unknown datadump syntax: "+syntax.trim());
			}
		}

		int i=1;
		for(;true;i++) {
			String sql = prop.getProperty("sqldump.query."+i+".sql");
			String tableName = prop.getProperty("sqldump.query."+i+".name");
			if(sql==null || tableName==null) { break; }

			Long tablerowlimit = Utils.getPropLong(prop, "sqldump.query."+i+".rowlimit");
			long rowlimit = tablerowlimit!=null?tablerowlimit:globalRowLimit!=null?globalRowLimit:Long.MAX_VALUE;

			try {
				log.debug("running query ["+i+", "+tableName+"]: "+sql);
				dd.runQuery(conn, sql, prop, tableName, charset, rowlimit, 
						dumpInsertInfoSyntax, dumpCSVSyntax, dumpJSONSyntax, 
						doColumnNamesDump, //insert into param
						doTableNameHeaderDump, doColumnNamesHeaderDump, columnDelimiter, recordDelimiter); //csv params
			} catch (Exception e) {
				log.warn("error on query "+i+": "+e);
				log.debug("error on query "+i+": "+e.getMessage(), e);
			}
		}
		log.info((i-1)+" queries runned");
	}
}
