package tbrugz.sqldump.datadump;

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import tbrugz.sqldump.def.AbstractSQLProc;
import tbrugz.sqldump.util.IOUtil;
import tbrugz.sqldump.util.ParametrizedProperties;
import tbrugz.sqldump.util.Utils;

//XXXdone: partition over (columnX) - different outputfiles for different values of columnX
//XXXdone: add prop: sqldump.query.<x>.file=/home/homer/query1.sql
//XXXxxx: add prop: sqldump.query.<x>.params=1,23,111 ; sqldump.query.<x>.param.pid_xx=1
//XXX: option to define bind parameters, eg., 'sqldump.query.<xxx>.bind.<yyy>=<zzz>'
//XXX?: add optional prop: sqldump.query.<x>.coltypes=Double, Integer, String, Double, ...
//XXXdone: add prop: sqldump.queries=q1,2,3,xxx (ids)
//XXXdone: option to log each 'n' rows dumped
//XXX: option to dump schema corresponding to queries data
public class SQLQueries extends AbstractSQLProc {
	
	static final String PROP_QUERIES = "sqldump.queries";

	static Log log = LogFactory.getLog(SQLQueries.class);
	
	@Override
	public void process() {
		DataDump dd = new DataDump();
		
		Long globalRowLimit = Utils.getPropLong(prop, DataDump.PROP_DATADUMP_ROWLIMIT);
		String charset = prop.getProperty(DataDump.PROP_DATADUMP_CHARSET, DataDump.CHARSET_DEFAULT);
		//boolean dumpInsertInfoSyntax = false, dumpCSVSyntax = false, dumpXMLSyntax = false, dumpJSONSyntax = false;
		
		String queriesStr = prop.getProperty(PROP_QUERIES);
		if(queriesStr==null) {
			log.warn("prop '"+PROP_QUERIES+"' not defined");
			return;
		}
		String[] queriesArr = queriesStr.split(",");
		int i=0;
		for(String qid: queriesArr) {
			qid = qid.trim();
			
			String queryName = prop.getProperty("sqldump.query."+qid+".name");
			List<DumpSyntax> syntaxList = getQuerySyntexes(qid);
			if(syntaxList==null) {
				log.warn("no dump syntax defined for query "+queryName+" [id="+qid+"]");
				continue;
			}
			//replace strings
			int replaceCount = 1;
			//List<String> replacers = new ArrayList<String>();
			while(true) {
				String paramStr = prop.getProperty("sqldump.query."+qid+".replace."+replaceCount);
				if(paramStr==null) { break; }
				prop.setProperty("sqldump.query.replace."+replaceCount, paramStr);
				//replacers.add(paramStr);
				replaceCount++;
			}
			//sql string
			String sql = prop.getProperty("sqldump.query."+qid+".sql");
			if(sql==null) {
				//load from file
				String sqlfile = prop.getProperty("sqldump.query."+qid+".sqlfile");
				if(sqlfile!=null) {
					sql = IOUtil.readFromFilename(sqlfile);
				}
				//replace props! XXX: replaceProps(): should be activated by a prop?
				sql = ParametrizedProperties.replaceProps(sql, prop);
			}
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
	
	List<DumpSyntax> getQuerySyntexes(String qid) {
		String syntaxes = prop.getProperty("sqldump.query."+qid+".dumpsyntaxes");
		if(syntaxes==null) {
			syntaxes = prop.getProperty(DataDump.PROP_DATADUMP_SYNTAXES);
		}
		if(syntaxes==null) {
			return null;
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
		return syntaxList;
	}
	
}
