package tbrugz.sqldump.datadump;

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import tbrugz.sqldump.JDBCSchemaGrabber;
import tbrugz.sqldump.dbmodel.Column;
import tbrugz.sqldump.dbmodel.Constraint;
import tbrugz.sqldump.dbmodel.Constraint.ConstraintType;
import tbrugz.sqldump.dbmodel.Query;
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
//XXXdone: option to grab/dump schema corresponding to queries data -> dbmodel.Query
public class SQLQueries extends AbstractSQLProc {
	
	static final String PROP_QUERIES = "sqldump.queries";
	static final String PROP_QUERIES_RUN = PROP_QUERIES+".runqueries";
	static final String PROP_QUERIES_ADD_TO_MODEL = PROP_QUERIES+".addtomodel";
	static final String PROP_QUERIES_SCHEMA = PROP_QUERIES+".schemaname";

	static final String DEFAULT_QUERIES_SCHEMA = "SQLQUERY"; //XXX: default schema to be current schema for dumping?
	
	static Log log = LogFactory.getLog(SQLQueries.class);
	
	@Override
	public void process() {
		DataDump dd = new DataDump();
		boolean runQueries = true;
		boolean addQueriesToModel = false;
		
		runQueries = Utils.getPropBool(prop, PROP_QUERIES_RUN, runQueries);
		addQueriesToModel = Utils.getPropBool(prop, PROP_QUERIES_ADD_TO_MODEL, addQueriesToModel);
		
		Long globalRowLimit = Utils.getPropLong(prop, DataDump.PROP_DATADUMP_ROWLIMIT);
		String charset = prop.getProperty(DataDump.PROP_DATADUMP_CHARSET, DataDump.CHARSET_DEFAULT);
		//boolean dumpInsertInfoSyntax = false, dumpCSVSyntax = false, dumpXMLSyntax = false, dumpJSONSyntax = false;
		
		String defaultSchemaName = prop.getProperty(PROP_QUERIES_SCHEMA, DEFAULT_QUERIES_SCHEMA);
		
		String queriesStr = prop.getProperty(PROP_QUERIES);
		if(queriesStr==null) {
			log.warn("prop '"+PROP_QUERIES+"' not defined");
			return;
		}
		String[] queriesArr = queriesStr.split(",");
		int i=0;
		int queriesGrabbed=0;
		//List<Query> queries = new ArrayList<Query>(); 
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
			if(sql==null || queryName==null) {
				log.warn("no SQL or name defined for query [id="+qid+"]");
				continue;
			}
			//params
			int paramCount = 1;
			List<String> params = new ArrayList<String>();
			while(true) {
				String paramStr = prop.getProperty("sqldump.query."+qid+".param."+paramCount);
				if(paramStr==null) { break; }
				params.add(paramStr);
				log.debug("added bind param #"+paramCount+": "+paramStr);
				paramCount++;
			}

			Long tablerowlimit = Utils.getPropLong(prop, "sqldump.query."+qid+".rowlimit");
			long rowlimit = tablerowlimit!=null?tablerowlimit:globalRowLimit!=null?globalRowLimit:Long.MAX_VALUE;
			
			List<String> partitionsBy = Utils.getStringListFromProp(prop, "sqldump.query."+qid+".partitionby", "\\|");
			if(partitionsBy!=null) {
				log.info("partitionby-patterns: "+partitionsBy); //XXX: move log into DataDump?
			}

			List<String> keyCols = Utils.getStringListFromProp(prop, "sqldump.query."+qid+".keycols", ",");

			// adding query to model
			ADD_QUERY_TO_MODEL:
			if(addQueriesToModel) {
				if(model==null) {
					log.warn("can't add query [id="+qid+"; name="+queryName+"]: model is null");
					break ADD_QUERY_TO_MODEL;
				}
				
				Query query = new Query();
				query.id = qid;
				query.setName(queryName);
				//add schemaName
				query.setSchemaName(prop.getProperty("sqldump.query."+qid+".schemaname", defaultSchemaName));
				
				query.query = sql;
				query.parameterValues = params;
				//XXX: add columns? query.setColumns(columns)...
				if(keyCols!=null) {
					Constraint cpk = new Constraint();
					cpk.type = ConstraintType.PK;
					cpk.uniqueColumns = keyCols;
					cpk.setName(JDBCSchemaGrabber.newNameFromTableName(queryName, JDBCSchemaGrabber.pkNamePattern));
					List<Constraint> lc = query.getConstraints(); 
					if(lc==null) {
						lc = new ArrayList<Constraint>();
						query.setConstraints(lc);
					}	
					lc.add(cpk);
				}
				
				List<String> allCols = Utils.getStringListFromProp(prop, "sqldump.query."+qid+".cols", ",");
				if(allCols!=null) {
					List<Column> cols = new ArrayList<Column>();
					for(String colspec: allCols) {
						String[] colparts = colspec.split(":");
						Column c = new Column();
						c.setName(colparts[0]);
						if(colparts.length>1) {
							c.type = colparts[1];
						}
						cols.add(c);
					}
					
					if(cols.size()>0) {
						query.setColumns(cols);
					}
					else {
						log.warn("error setting cols for query [id="+qid+"; name="+queryName+"]");
					}
				}

				//queries.add(query);
				queriesGrabbed++;
				model.getViews().add(query);
			}
			// added query to model, or not
			
			if(runQueries) {
				try {
					log.debug("running query [id="+qid+"; name="+queryName+"]: "+sql);
					dd.runQuery(conn, sql, params, prop, qid, queryName, charset, rowlimit, syntaxList, 
							partitionsBy!=null ? partitionsBy.toArray(new String[]{}) : null, 
							keyCols, null, null);
				} catch (Exception e) {
					log.warn("error on query '"+qid+"'\n... sql: "+sql+"\n... exception: "+String.valueOf(e).trim());
					log.info("error on query "+qid+": "+e.getMessage(), e);
				}
			}
			i++;
		}
		
		if(runQueries) {
			log.info(i+" queries runned");
		}
		if(addQueriesToModel) {
			log.info(queriesGrabbed+" queries grabbed from properties");
		}
		if(!runQueries && !addQueriesToModel) {
			log.warn("no queries runned or grabbed");
		}
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
			for(Class<? extends DumpSyntax> dsc: DumpSyntaxRegistry.getSyntaxes()) {
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
