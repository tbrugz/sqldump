package tbrugz.sqldump;

import java.io.FileReader;
import java.io.IOException;
import java.io.StringWriter;
import java.sql.Connection;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import org.apache.log4j.Logger;

import tbrugz.sqldump.datadump.DumpSyntax;

//XXX: partition over (columnX) - different outputfiles for different values of columnX
//XXXdone: add prop: sqldump.query.<x>.file=/home/homer/query1.sql
//XXXxxx: add prop: sqldump.query.<x>.params=1,23,111
//XXX: add prop: sqldump.query.<x>.param.pid_xx=1
//XXX?: add optional prop: sqldump.query.<x>.coltypes=Double, Integer, String, Double, ...
//XXXdone: add prop: sqldump.queries=q1,2,3,xxx (ids)
//XXX: option to log each 'n' rows dumped
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
			boolean syntaxAdded = false;
			for(Class<? extends DumpSyntax> dsc: DumpSyntax.getSyntaxes()) {
				DumpSyntax ds = (DumpSyntax) Utils.getClassInstance(dsc);
				if(ds!=null && ds.getSyntaxId().equals(syntax.trim())) {
					ds.procProperties(prop);
					syntaxList.add(ds);
					syntaxAdded = true;
				}
			}
			if(!syntaxAdded) {
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
				String sqlfile = prop.getProperty("sqldump.query."+qid+".sqlfile");
				if(sqlfile!=null) {
					sql = readFile(sqlfile);
				}
			}
			String queryName = prop.getProperty("sqldump.query."+qid+".name");
			if(sql==null || queryName==null) { break; }
			//params
			int paramCount = 1;
			List<String> params = new ArrayList<String>();
			while(true) {
				String paramStr = prop.getProperty("sqldump.query."+qid+".param."+paramCount);
				if(paramStr==null) { break; }
				params.add(paramStr);
				paramCount++;
			}

			Long tablerowlimit = Utils.getPropLong(prop, "sqldump.query."+qid+".rowlimit");
			long rowlimit = tablerowlimit!=null?tablerowlimit:globalRowLimit!=null?globalRowLimit:Long.MAX_VALUE;
			
			String partitionBy = prop.getProperty("sqldump.query."+qid+".partitionby");

			List<String> keyCols = Utils.getStringListFromProp(prop, "sqldump.query."+qid+".keycols", ",");
			
			try {
				log.debug("running query [id="+qid+"; name="+queryName+"]: "+sql);
				dd.runQuery(conn, sql, params, prop, qid, queryName, charset, rowlimit, syntaxList, partitionBy, keyCols);
			} catch (Exception e) {
				log.warn("error on query '"+qid+"'\n... sql: "+sql+"\n... exception: "+String.valueOf(e).trim());
				log.info("error on query "+qid+": "+e.getMessage(), e);
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
