package tbrugz.sqldump;

import java.io.FileReader;
import java.io.IOException;
import java.io.StringWriter;
import java.sql.Connection;
import java.util.Properties;

import org.apache.log4j.Logger;

//XXX: partition over (columnX) - different outputfiles for different values of columnX
//XXXdone: add prop: sqldump.query.<x>.file=/home/homer/query1.sql
//XXX: add prop: sqldump.query.<x>.params=1,23,111
//XXX: add prop: sqldump.query.<x>.param.pid_xx=1
//XXX?: add prop: sqldump.query.<x>.coltypes=Double, Integer, String, Double, ...
//XXX: add prop: sqldump.queries=q1,2,3,xxx (ids)
public class SQLQueries {

	static Logger log = Logger.getLogger(SQLQueries.class);
	
	public static void doQueries(Connection conn, Properties prop) {
		DataDump dd = new DataDump();
		
		Long globalRowLimit = Utils.getPropLong(prop, DataDump.PROP_DATADUMP_ROWLIMIT);
		String recordDelimiter = prop.getProperty(DataDump.PROP_DATADUMP_RECORDDELIMITER, DataDump.DELIM_RECORD_DEFAULT);
		String columnDelimiter = prop.getProperty(DataDump.PROP_DATADUMP_COLUMNDELIMITER, DataDump.DELIM_COLUMN_DEFAULT);
		String charset = prop.getProperty(DataDump.PROP_DATADUMP_CHARSET, DataDump.CHARSET_DEFAULT);
		boolean doTableNameHeaderDump = Utils.getPropBool(prop, DataDump.PROP_DATADUMP_TABLENAMEHEADER);
		boolean doColumnNamesHeaderDump = Utils.getPropBool(prop, DataDump.PROP_DATADUMP_COLUMNNAMESHEADER);
		boolean doColumnNamesDump = Utils.getPropBool(prop, DataDump.PROP_DATADUMP_INSERTINTO_WITHCOLNAMES);
		boolean dumpInsertInfoSyntax = false, dumpCSVSyntax = false, dumpXMLSyntax = false, dumpJSONSyntax = false;
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
			else if(DataDump.SYNTAX_XML.equals(syntax.trim())) {
				dumpXMLSyntax = true;
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
			if(sql==null) {
				//load from file
				String sqlfile = prop.getProperty("sqldump.query."+i+".file");
				if(sqlfile!=null) {
					sql = readFile(sqlfile);
				}
			}
			String tableName = prop.getProperty("sqldump.query."+i+".name");
			if(sql==null || tableName==null) { break; }

			Long tablerowlimit = Utils.getPropLong(prop, "sqldump.query."+i+".rowlimit");
			long rowlimit = tablerowlimit!=null?tablerowlimit:globalRowLimit!=null?globalRowLimit:Long.MAX_VALUE;

			try {
				log.debug("running query ["+i+", "+tableName+"]: "+sql);
				dd.runQuery(conn, sql, prop, tableName, charset, rowlimit, 
						dumpInsertInfoSyntax, dumpCSVSyntax, dumpXMLSyntax, dumpJSONSyntax, 
						doColumnNamesDump, //insert into param
						doTableNameHeaderDump, doColumnNamesHeaderDump, columnDelimiter, recordDelimiter); //csv params
			} catch (Exception e) {
				log.warn("error on query "+i+": "+e);
				log.debug("error on query "+i+": "+e.getMessage(), e);
			}
		}
		i--;
		log.info(i+" queries runned");
	}
	
	static String readFile(String fileName) {
		try {
	
			FileReader reader = new FileReader(fileName);			
			StringWriter sw = new StringWriter();
			char[] cbuf = new char[4096];
			int iread = reader.read(cbuf);
			
			while(iread>0) {
				sw.write(cbuf, 0, iread);
				iread = reader.read(cbuf);
			}
	
			return sw.toString();
	
		} catch (IOException e) {
			log.warn("error reading file "+fileName+": "+e.getMessage());
		}
		return null;
	}
	
}
