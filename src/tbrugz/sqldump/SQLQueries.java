package tbrugz.sqldump;

import java.io.FileReader;
import java.io.IOException;
import java.io.StringWriter;
import java.sql.Connection;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import org.apache.log4j.Logger;

import tbrugz.sqldump.datadump.CSVDataDump;
import tbrugz.sqldump.datadump.DumpSyntax;
import tbrugz.sqldump.datadump.InsertIntoDataDump;
import tbrugz.sqldump.datadump.JSONDataDump;
import tbrugz.sqldump.datadump.XMLDataDump;

//XXX: partition over (columnX) - different outputfiles for different values of columnX
//XXXdone: add prop: sqldump.query.<x>.file=/home/homer/query1.sql
//XXX: add prop: sqldump.query.<x>.params=1,23,111
//XXX: add prop: sqldump.query.<x>.param.pid_xx=1
//XXX?: add optional prop: sqldump.query.<x>.coltypes=Double, Integer, String, Double, ...
//XXXdone: add prop: sqldump.queries=q1,2,3,xxx (ids)
public class SQLQueries {
	
	static final String PROP_QUERIES = "sqldump.queries";

	static Logger log = Logger.getLogger(SQLQueries.class);
	
	public static void doQueries(Connection conn, Properties prop) {
		DataDump dd = new DataDump();
		
		Long globalRowLimit = Utils.getPropLong(prop, DataDump.PROP_DATADUMP_ROWLIMIT);
		String charset = prop.getProperty(DataDump.PROP_DATADUMP_CHARSET, DataDump.CHARSET_DEFAULT);
		//boolean dumpInsertInfoSyntax = false, dumpCSVSyntax = false, dumpXMLSyntax = false, dumpJSONSyntax = false;
		String syntaxes = prop.getProperty(DataDump.PROP_DATADUMP_SYNTAXES);
		if(syntaxes==null) {
			log.warn("no datadump syntax defined");
			return;
		}
		String[] syntaxArr = syntaxes.split(",");
		List<DumpSyntax> syntaxList = new ArrayList<DumpSyntax>();
		for(String syntax: syntaxArr) {
			if(DataDump.SYNTAX_INSERTINTO.equals(syntax.trim())) {
				InsertIntoDataDump dtd = new InsertIntoDataDump();
				dtd.procProperties(prop);
				syntaxList.add(dtd);
			}
			else if(DataDump.SYNTAX_CSV.equals(syntax.trim())) {
				CSVDataDump dtd = new CSVDataDump();
				dtd.procProperties(prop);
				syntaxList.add(dtd);
			}
			else if(DataDump.SYNTAX_XML.equals(syntax.trim())) {
				XMLDataDump dtd = new XMLDataDump();
				dtd.procProperties(prop);
				syntaxList.add(dtd);
			}
			else if(DataDump.SYNTAX_JSON.equals(syntax.trim())) {
				JSONDataDump dtd = new JSONDataDump();
				dtd.procProperties(prop);
				syntaxList.add(dtd);
			}
			else {
				log.warn("unknown datadump syntax: "+syntax.trim());
			}
		}

		String queriesStr = prop.getProperty(PROP_QUERIES);
		if(queriesStr==null) {
			log.warn("prop '"+PROP_QUERIES+"' not defined");
			return;
		}
		String[] queriesArr = queriesStr.split(",");
		int i=0;
		for(String qid: queriesArr) {
			qid = qid.trim();
			String sql = prop.getProperty("sqldump.query."+qid+".sql");
			if(sql==null) {
				//load from file
				String sqlfile = prop.getProperty("sqldump.query."+qid+".file");
				if(sqlfile!=null) {
					sql = readFile(sqlfile);
				}
			}
			String tableName = prop.getProperty("sqldump.query."+qid+".name");
			if(sql==null || tableName==null) { break; }

			Long tablerowlimit = Utils.getPropLong(prop, "sqldump.query."+qid+".rowlimit");
			long rowlimit = tablerowlimit!=null?tablerowlimit:globalRowLimit!=null?globalRowLimit:Long.MAX_VALUE;

			try {
				log.debug("running query ["+qid+", "+tableName+"]: "+sql);
				dd.runQuery(conn, sql, prop, tableName, charset, rowlimit, syntaxList);
			} catch (Exception e) {
				log.warn("error on query "+qid+": "+e);
				log.debug("error on query "+qid+": "+e.getMessage(), e);
			}
			i++;
		}
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
