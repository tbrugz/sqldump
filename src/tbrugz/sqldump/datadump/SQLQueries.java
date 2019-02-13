package tbrugz.sqldump.datadump;

import java.io.File;
import java.io.FilenameFilter;
import java.sql.ParameterMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.TreeMap;
import java.util.regex.Pattern;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import tbrugz.sqldump.dbmd.DBMSFeatures;
import tbrugz.sqldump.dbmodel.Column;
import tbrugz.sqldump.dbmodel.Constraint;
import tbrugz.sqldump.dbmodel.DBIdentifiable;
import tbrugz.sqldump.dbmodel.Grant;
import tbrugz.sqldump.dbmodel.PrivilegeType;
import tbrugz.sqldump.dbmodel.TableType;
import tbrugz.sqldump.dbmodel.View;
import tbrugz.sqldump.dbmodel.Constraint.ConstraintType;
import tbrugz.sqldump.dbmodel.Query;
import tbrugz.sqldump.dbmodel.Table;
import tbrugz.sqldump.def.AbstractSQLProc;
import tbrugz.sqldump.def.DBMSResources;
import tbrugz.sqldump.def.ProcessingException;
import tbrugz.sqldump.resultset.ResultSetDecoratorFactory;
import tbrugz.sqldump.util.IOUtil;
import tbrugz.sqldump.util.ParametrizedProperties;
import tbrugz.sqldump.util.SQLUtils;
import tbrugz.sqldump.util.StringUtils;
import tbrugz.sqldump.util.Utils;

//XXXdone: partition over (columnX) - different outputfiles for different values of columnX
//XXXdone: add prop: sqldump.query.<x>.file=/home/homer/query1.sql
//XXXxxx: add prop: sqldump.query.<x>.params=1,23,111 ; sqldump.query.<x>.param.pid_xx=1
//XXXdone: option to define bind parameters, eg., 'sqldump.query.<xxx>.bind.<yyy>=<zzz>' - see '.param'
//XXXdone: add optional prop: sqldump.query.<x>.coltypes=Double, Integer, String, Double, ... - see '.cols'
//XXXdone: add prop: sqldump.queries=q1,2,3,xxx (ids)
//XXXdone: option to log each 'n' rows dumped
//XXXdone: option to grab/dump schema corresponding to queries data -> dbmodel.Query
public class SQLQueries extends AbstractSQLProc {
	
	protected static final String PROP_QUERIES = "sqldump.queries";
	protected static final String PROP_QUERIES_RUN = PROP_QUERIES+".runqueries";
	protected static final String PROP_QUERIES_ADD_TO_MODEL = PROP_QUERIES+".addtomodel";
	protected static final String PROP_QUERIES_SCHEMA = PROP_QUERIES+".schemaname";
	protected static final String PROP_QUERIES_GRABCOLSINFOFROMMETADATA = PROP_QUERIES+".grabcolsinfofrommetadata";

	protected static final String PROP_QUERIES_FROM_DIR = PROP_QUERIES+".from-dir";
	
	protected static final String PREFIX_QUERY = "sqldump.query.";

	protected static final String DEFAULT_QUERIES_SCHEMA = "SQLQUERY"; //XXX: default schema to be current schema for dumping?
	
	protected static final String ROLES_DELIMITER_STR = "|";
	protected static final String ROLES_DELIMITER = Pattern.quote(ROLES_DELIMITER_STR);
	
	static final Log log = LogFactory.getLog(SQLQueries.class);
	
	public static class PropertiesWithoutNPE extends Properties {
		private static final long serialVersionUID = 1L;

		@Override
		public synchronized Object put(Object key, Object value) {
			if(value==null) { return null; };
			return super.put(key, value);
		}
	}
	
	public static class SqlFilenameFilter implements FilenameFilter {

		static final String SQL_EXT = ".sql";
		
		@Override
		public boolean accept(File dir, String name) {
			return name.endsWith(SQL_EXT);
		}
	}
	
	@Override
	public void process() {
		boolean runQueries = true;
		boolean addQueriesToModel = false;
		
		runQueries = Utils.getPropBool(prop, PROP_QUERIES_RUN, runQueries);
		addQueriesToModel = Utils.getPropBool(prop, PROP_QUERIES_ADD_TO_MODEL, addQueriesToModel);
		
		Long globalRowLimit = Utils.getPropLong(prop, DataDump.PROP_DATADUMP_ROWLIMIT);
		String charset = prop.getProperty(DataDump.PROP_DATADUMP_CHARSET, DataDump.CHARSET_DEFAULT);
		//boolean dumpInsertInfoSyntax = false, dumpCSVSyntax = false, dumpXMLSyntax = false, dumpJSONSyntax = false;
		
		String defaultSchemaName = prop.getProperty(PROP_QUERIES_SCHEMA, DEFAULT_QUERIES_SCHEMA);
		
		int i=0;
		int queriesGrabbed=0;
		
		Map<String, Properties> qmap = getQueryPropMap();
		
		//List<Query> queries = new ArrayList<Query>(); 
		//for(String qid: queriesArr) {
		String queriesStr = prop.getProperty(PROP_QUERIES);
		if(queriesStr!=null) {
			log.debug("prop '"+PROP_QUERIES+"': "+queriesStr);
		}
		log.debug("query ids: "+qmap.keySet());
		
		for(Map.Entry<String, Properties> qentry: qmap.entrySet()) {
			String qid = qentry.getKey();
			Properties qp = qentry.getValue();
			//log.debug("query '"+qid+"' props: "+qp);
			
			if(qp==null) {
				log.warn("no SQL defined for query [id="+qid+";propkey='"+PREFIX_QUERY+qid+".sql(file)"+"']");
				continue;
			}
			
			String queryName = qp.getProperty("name");
			String schemaName = qp.getProperty("schemaname", defaultSchemaName);
			DBMSFeatures feat = null;
			if(model!=null) {
				feat = DBMSResources.instance().getSpecificFeatures(model.getSqlDialect());
			}
			else {
				try {
					feat = DBMSResources.instance().getSpecificFeatures(conn.getMetaData());
				}
				catch(SQLException e) {
					throw new ProcessingException("Error on getSpecificFeatures()", e);
				}
			}
			String syntaxes = qp.getProperty("dumpsyntaxes");
			List<DumpSyntax> syntaxList = getQuerySyntaxes(syntaxes, feat);
			if(runQueries && syntaxList==null) {
				log.warn("no dump syntax defined for query "+queryName+" [id="+qid+"]");
				continue;
			}
			/*
			//replace strings
			int replaceCount = 1;
			//List<String> replacers = new ArrayList<String>();
			while(true) {
				String paramStr = qp.getProperty("replace."+replaceCount);
				if(paramStr==null) { break; }
				prop.setProperty("sqldump.query.replace."+replaceCount, paramStr);
				//replacers.add(paramStr);
				replaceCount++;
			}
			*/
			//sql string
			String sql = qp.getProperty("sql");
			/*if(sql==null) {
				log.warn("no SQL defined for query [id="+qid+";propkey='"+PREFIX_QUERY+qid+".sql(file)"+"']");
				continue;
			}*/
			
			PreparedStatement stmt = null;
			try {
				String finalSql = processQuery(sql);
				stmt = conn.prepareStatement(finalSql);
			} catch (SQLException e) {
				String message = "error creating prepared statement [id="+qid+";sql="+sql+"]: "+e.getMessage();
				log.warn(message);
				if(failonerror) {
					throw new ProcessingException(message, e);
				}
				//continue; //?
			}
			
			//bind params
			int paramCount = 1;
			List<Object> params = new ArrayList<Object>();
			while(true) {
				String paramStr = qp.getProperty("param."+paramCount);
				if(paramStr==null) { break; }
				params.add(paramStr);
				log.debug("added bind param #"+paramCount+": "+paramStr);
				paramCount++;
			}

			Long tablerowlimit = Utils.getPropLong(qp, "rowlimit");
			long rowlimit = tablerowlimit!=null?tablerowlimit:globalRowLimit!=null?globalRowLimit:Long.MAX_VALUE;
			
			List<String> partitionsBy = Utils.getStringListFromProp(qp, "partitionby", "\\|");

			List<String> keyCols = Utils.getStringListFromProp(qp, "keycols", ",");
			
			ResultSetDecoratorFactory rsdf = null;
			String rsDecoratorFactory = qp.getProperty("rsdecoratorfactory");
			String rsArgPrepend = "rsdecorator.";
			List<String> rsFactoryArgs = Utils.getKeysStartingWith(qp, rsArgPrepend);
			if(rsDecoratorFactory!=null) {
				rsdf = (ResultSetDecoratorFactory) Utils.getClassInstance(rsDecoratorFactory, "tbrugz.sqldump.resultset", null);
				for(String arg: rsFactoryArgs) {
					rsdf.set(arg.substring(rsArgPrepend.length()), qp.getProperty(arg));
				}
			}

			List<String> colNamesToDump = Utils.getStringListFromProp(qp, "dump-cols", ",");
			
			// adding query to model
			if(addQueriesToModel) {
				String remarks = qp.getProperty("remarks");
				String roles = qp.getProperty("roles");
				String cols = qp.getProperty("cols");
				queriesGrabbed += addQueryToModelInternal(qid, queryName, schemaName, stmt, sql, keyCols, cols, params, remarks, roles, rsDecoratorFactory, rsFactoryArgs, rsArgPrepend);
			}
			
			if(runQueries && stmt!=null) {
				try {
					log.debug("running query [id="+qid+"; name="+queryName+"]: "+sql);
					DataDump dd = new DataDump();
					
					Integer fetchSize = Utils.getPropInt(prop, DataDump.PROP_DATADUMP_FETCHSIZE);
					if(fetchSize!=null) {
						log.debug("[qid="+qid+"] setting fetch size: "+fetchSize);
						stmt.setFetchSize(fetchSize);
					}
					
					dd.runQuery(conn, stmt, params, prop, schemaName, qid, queryName, charset, rowlimit, syntaxList,
						partitionsBy, keyCols, null, null, rsdf, colNamesToDump);
				} catch (Exception e) {
					log.warn("error on query '"+qid+"'\n... sql: "+sql+"\n... exception: "+String.valueOf(e).trim());
					log.debug("error on query "+qid+" [class="+e.getClass().getName()+"]: "+e.getMessage(), e);
					if(log.isDebugEnabled() && (e instanceof SQLException)) {
						SQLUtils.xtraLogSQLException((SQLException) e, log);
					}
					if(failonerror) {
						throw new ProcessingException(e);
					}
				}
			}
			i++;
		}
		
		if(runQueries) {
			log.info(i+" queries ran");
		}
		if(addQueriesToModel) {
			log.info(queriesGrabbed+" queries grabbed from properties");
		}
		if(!runQueries && !addQueriesToModel) {
			log.warn("no queries ran or grabbed");
		}
	}
	
	Map<String, Properties> getQueryPropMap() {
		Map<String, Properties> ret = new LinkedHashMap<String, Properties>();
		
		String queriesStr = prop.getProperty(PROP_QUERIES);
		String queriesDir = prop.getProperty(PROP_QUERIES_FROM_DIR);
		
		List<String> qids = null;
		if(queriesStr!=null) {
			qids = new ArrayList<String>();
			String[] queriesArr = queriesStr.split(",");
			for(String qid: queriesArr) {
				qid = qid.trim();
				qids.add(qid);
				
				//Properties qp = getQueryProperties(qid);
				//ret.put(qid, qp);
			}
		}
		
		List<String> fids = null;
		List<File> files = new ArrayList<File>();
		
		if(queriesDir!=null) {
			fids = new ArrayList<String>();
			File dir = new File(queriesDir);
			if(dir.exists()) {
				File[] filesz = dir.listFiles(new SqlFilenameFilter());
				List<String> baseNames = new ArrayList<String>();
				for(File f: filesz) {
					String fn = f.getName();
					String baseName = fn.substring(0, fn.length() - SqlFilenameFilter.SQL_EXT.length());
					baseNames.add(baseName);
				}
				if(qids!=null) {
					for(String qid: qids) {
						File f = new File(dir, qid+SqlFilenameFilter.SQL_EXT);
						if(baseNames.contains(qid)) {
							files.add(f);
							fids.add(qid);
						}
						else {
							log.debug("file '"+f.getAbsolutePath()+"' [qid="+qid+"]  not in filenames: "+baseNames);
						}
					}
					//List<String> qidsXtra = new ArrayList<String>();
					//qidsXtra.addAll(qids);
					//qidsXtra.removeAll(fids);
					//if(qidsXtra.size()>0) {
					qids.removeAll(fids);
					if(qids.size()>0) {
						log.debug("query ids not found in dir '"+dir.getPath()+"': "+qids);
					}
				}
				else {
					for(String baseName: baseNames) {
						files.add(new File(dir, baseName+SqlFilenameFilter.SQL_EXT));
						fids.add(baseName);
					}
				}
			}
			else {
				log.warn("dir not found: "+dir.getPath());
			}
		}

		if(fids!=null) {
			for(int i=0;i<fids.size();i++) {
				String fid = fids.get(i);
				Properties qp = getQueryProperties(fid);
				
				String sql = IOUtil.readFromFilename(files.get(i).getAbsolutePath());
				sql = ParametrizedProperties.replaceProps(sql, prop);
				if(qp.getProperty("sql")!=null) {
					log.warn("property 'sql' from query '"+fid+"' will be ignored; file '"+files.get(i).getAbsolutePath()+"' will be used");
				}
				qp.put("sql", sql);
				//XXX add <path>/<qid>.properties
				
				ret.put(fid, qp);
			}
		}
		//else {
		if(qids!=null) {
			for(String qid: qids) {
				Properties qp = getQueryProperties(qid);
				if(qp==null) {
					log.warn("query '"+qid+"': no properties found");
				}
				else {
					ret.put(qid, qp);
				}
			}
		}
		
		if(ret.size()==0 || (qids==null && fids==null)) {
			String message = "no queries defined [prop '"+PROP_QUERIES+"'="+queriesStr+";qids="+qids+";fids="+fids+"]";
			log.error(message);
			if(failonerror) {
				throw new ProcessingException("SQLQueries: "+message);
			}
			return null;
		}
		return ret;
	}
	
	Properties getQueryProperties(String qid) {
		Properties ret = new PropertiesWithoutNPE();

		//sql string
		String sql = prop.getProperty(PREFIX_QUERY+qid+".sql");
		if(sql==null) {
			//load from file
			String sqlfile = prop.getProperty(PREFIX_QUERY+qid+".sqlfile");
			if(sqlfile!=null) {
				sql = IOUtil.readFromFilename(sqlfile);
			}
			//replace props! XXX: replaceProps(): should be activated by a prop?
			sql = ParametrizedProperties.replaceProps(sql, prop);
		}
		if(sql==null) {
			log.warn("no SQL defined for query [id="+qid+";propkey='"+PREFIX_QUERY+qid+".sql(file)"+"']");
			return null;
		}
		
		//query properties
		String queryName = prop.getProperty(PREFIX_QUERY+qid+".name");
		if(queryName==null) {
			log.info("no name defined for query [id="+qid+"] (query name will be equal to id)");
			queryName = qid;
		}
		ret.setProperty("name", queryName);
		
		//String syntaxes = prop.getProperty(PREFIX_QUERY+qid+".dumpsyntaxes");
		ret.setProperty("dumpsyntaxes", prop.getProperty(PREFIX_QUERY+qid+".dumpsyntaxes"));
		//replace strings
		int replaceCount = 1;
		//List<String> replacers = new ArrayList<String>();
		//XXX deprecate '.replace'?
		while(true) {
			String paramStr = prop.getProperty(PREFIX_QUERY+qid+".replace."+replaceCount);
			if(paramStr==null) { break; }
			log.info("replace:: "+replaceCount+" / "+paramStr);
			ret.setProperty("sqldump.query.replace."+replaceCount, paramStr);
			//replacers.add(paramStr);
			replaceCount++;
		}
		replaceCount--;
		if(replaceCount>0) {
			sql = ParametrizedProperties.replaceProps(sql, ret);
			log.info("replacing with 'sqldump.query.replace.<1-n>' [replaceCount="+replaceCount+"]");
		}
		ret.setProperty("sql", sql);
		
		//bind params
		int paramCount = 1;
		//List<Object> params = new ArrayList<Object>();
		while(true) {
			String paramStr = prop.getProperty(PREFIX_QUERY+qid+".param."+paramCount);
			if(paramStr==null) { break; }
			//params.add(paramStr);
			ret.setProperty("param."+paramCount, paramStr);
			log.debug("added bind param #"+paramCount+": "+paramStr);
			paramCount++;
		}

		ret.setProperty("rowlimit", prop.getProperty(PREFIX_QUERY+qid+".rowlimit"));
		ret.setProperty("partitionby", prop.getProperty(PREFIX_QUERY+qid+".partitionby"));
		ret.setProperty("keycols", prop.getProperty(PREFIX_QUERY+qid+".keycols"));
		ret.setProperty("schemaname", prop.getProperty(PREFIX_QUERY+qid+".schemaname"));
		ret.setProperty("cols", prop.getProperty(PREFIX_QUERY+qid+".cols"));

		{
			String rsDecoratorFactory = prop.getProperty(PREFIX_QUERY+qid+".rsdecoratorfactory");
			ret.setProperty("rsdecoratorfactory", rsDecoratorFactory);
			String rsArgPrepend = PREFIX_QUERY+qid+".rsdecorator.";
			List<String> rsFactoryArgs = Utils.getKeysStartingWith(prop, rsArgPrepend);
			if(rsDecoratorFactory!=null) {
				for(String arg: rsFactoryArgs) {
					ret.setProperty(arg.substring(rsArgPrepend.length()), prop.getProperty(arg));
				}
			}
		}

		ret.setProperty("dump-cols", prop.getProperty(PREFIX_QUERY+qid+".dump-cols"));
		ret.setProperty("remarks", prop.getProperty(PREFIX_QUERY+qid+".remarks"));
		ret.setProperty("roles", prop.getProperty(PREFIX_QUERY+qid+".roles"));
		
		return ret;
	}
	
	List<DumpSyntax> getQuerySyntaxes(String syntaxes, DBMSFeatures feat) {
		if(syntaxes==null) {
			syntaxes = prop.getProperty(DataDump.PROP_DATADUMP_SYNTAXES);
		}
		if(syntaxes==null) {
			return null;
		}
		String[] syntaxArr = syntaxes.split(",");
		List<DumpSyntax> syntaxList = new ArrayList<DumpSyntax>();
		boolean allSyntaxesAdded = true;
		for(String syntax: syntaxArr) {
			boolean syntaxAdded = false;
			for(Class<DumpSyntax> dsc: DumpSyntaxRegistry.getSyntaxes()) {
				DumpSyntax ds = (DumpSyntax) Utils.getClassInstance(dsc);
				if(ds!=null && ds.getSyntaxId().equals(syntax.trim())) {
					ds.procProperties(prop);
					if(ds.needsDBMSFeatures()) { ds.setFeatures(feat); }
					syntaxList.add(ds);
					syntaxAdded = true;
				}
			}
			if(!syntaxAdded) {
				log.warn("unknown datadump syntax: "+syntax.trim());
				allSyntaxesAdded = false;
			}
		}
		if(!allSyntaxesAdded) {
			log.info("not all syntaxes added... syntaxes avaiable: " + StringUtils.getClassSimpleNameList(DumpSyntaxRegistry.getSyntaxes()) );
		}
		return syntaxList;
	}
	
	int addQueryToModelInternal(String qid, String queryName, String schemaName,
			PreparedStatement stmt, String sql, List<String> keyCols, String colNames,
			List<Object> params, String remarks, String roles,
			String rsDecoratorFactory, List<String> rsFactoryArgs, String rsArgPrepend) {
		
		//String schemaName = prop.getProperty(PREFIX_QUERY+qid+".schemaname", defaultSchemaName);
		//String colNames = prop.getProperty(PREFIX_QUERY+qid+".cols");
		boolean grabInfoFromMetadata = Utils.getPropBool(prop, PROP_QUERIES_GRABCOLSINFOFROMMETADATA, false);
		
		//XXX: add prop for 'addAlsoAsTable'? default is false
		return addQueryToModel(qid, queryName, schemaName, colNames, grabInfoFromMetadata, /*addAlsoAsTable*/ false, stmt, sql, keyCols, params, remarks, roles, rsDecoratorFactory, rsFactoryArgs, rsArgPrepend);
	}
	
	public int addQueryToModel(String qid, String queryName, String schemaName,
			String colNames, boolean grabInfoFromMetadata, boolean addAlsoAsTable,
			PreparedStatement stmt, String sql, List<String> keyCols,
			List<Object> params, String remarks, String roles,
			String rsDecoratorFactory, List<String> rsFactoryArgs, String rsArgPrepend) {
		
		if(model==null) {
			log.warn("can't add query [id="+qid+"; name="+queryName+"]: model is null");
			return 0;
		}
		
		Query query = new Query();
		query.setId(qid);
		query.setName(queryName);
		//add schemaName
		query.setSchemaName(schemaName);
		
		query.setQuery(sql);
		query.setParameterValues(params);
		query.setRemarks(remarks);
		setQueryRoles(query, roles);
		
		//XXX: add columns? query.setColumns(columns)...
		if(keyCols!=null) {
			Constraint cpk = new Constraint();
			cpk.setType(ConstraintType.PK);
			cpk.setUniqueColumns(keyCols);
			cpk.setName(SQLUtils.newNameFromTableName(queryName, SQLUtils.pkNamePattern));
			List<Constraint> lc = query.getConstraints(); 
			if(lc==null) {
				lc = new ArrayList<Constraint>();
				query.setConstraints(lc);
			}	
			lc.add(cpk);
		}
		
		List<String> allCols = Utils.getStringList(colNames, ",");
		if(allCols!=null) {
			List<Column> cols = new ArrayList<Column>();
			for(String colspec: allCols) {
				String[] colparts = colspec.split(":");
				Column c = new Column();
				c.setName(colparts[0]);
				if(colparts.length>1) {
					c.setType(colparts[1]);
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
		else {
			//getting columns from prepared statement metadata
			if(grabInfoFromMetadata && stmt!=null) {
				log.debug("grabbing colums name & type from prepared statement's metadata [id="+qid+"; name="+queryName+"]");
				try {
					ResultSetMetaData rsmd = stmt.getMetaData();
					if(rsmd!=null) {
						query.setColumns(DataDumpUtils.getColumns(rsmd));
					}
					else {
						log.warn("getMetaData() returned null: empty query? sql:\n"+sql);
					}
				} catch (SQLException e) {
					try {
						stmt.getConnection().rollback();
					} catch (SQLException e1) {
						log.warn("Error rolling back: "+e);
					}
					query.setColumns(new ArrayList<Column>());
					//query.setColumns(null); //XXX null is better??
					log.warn("resultset metadata's sqlexception: "+e.toString().trim());
					log.debug("resultset metadata's sqlexception: "+e.getMessage(), e);
				}
				
				try {
					//XXX option to set parameter count by properties?
					ParameterMetaData pmd = stmt.getParameterMetaData();
					if(pmd!=null) {
						query.setParameterCount(pmd.getParameterCount());
					}
					//XXX set parameter type names??
				} catch (SQLException e) {
					try {
						stmt.getConnection().rollback();
					} catch (SQLException e1) {
						log.warn("Error rolling back: "+e);
					}
					query.setParameterCount(null);
					log.warn("parameter metadata's sqlexception: "+e.toString().trim());
					log.debug("parameter metadata's sqlexception: "+e.getMessage(), e);
				}
			}
			if(grabInfoFromMetadata && stmt==null) {
				log.warn("statement is null: can't grab metadata [id="+qid+"; name="+queryName+"]");
			}
		}
		
		if(rsDecoratorFactory!=null) {
			query.setRsDecoratorFactoryClass(rsDecoratorFactory);
			query.setRsDecoratorArguments(new TreeMap<String, String>());
			for(String arg: rsFactoryArgs) {
				query.getRsDecoratorArguments().put(arg.substring(rsArgPrepend.length()), prop.getProperty(arg));
			}
		}
		
		View v = DBIdentifiable.getDBIdentifiableByName(model.getViews(), query.getName());
		if(v!=null) {
			boolean removed = model.getViews().remove(v);
			log.info("removed query '"+v+"'? "+removed);
		}
		boolean added = model.getViews().add(query);
		log.debug("added query '"+query+"'? "+added);

		if(addAlsoAsTable) {
			//adding view to table's list
			Table t = new Table();
			t.setSchemaName(query.getSchemaName());
			t.setName(query.getName());
			t.setType(TableType.VIEW);
			t.setColumns(query.getColumns());
			model.getTables().add(t);
		}
		
		return added?1:0;
	}
	
	protected String processQuery(String sql) {
		return sql;
	}
	
	@Deprecated
	protected static void addRolesToQuery(Query query, String rolesFilterStr) {
		setQueryRoles(query, rolesFilterStr);
	}
	
	protected static void setQueryRoles(Query query, String rolesFilterStr) {
		List<String> rolesFilter = Utils.getStringList(rolesFilterStr, ROLES_DELIMITER);
		if(rolesFilter==null || rolesFilter.size()==0) {
			return;
		}
		List<Grant> grants = new ArrayList<Grant>();
		for(String role: rolesFilter) {
			if(role!=null && !role.trim().equals("")) {
				grants.add(new Grant(query.getName(), PrivilegeType.SELECT, role));
			}
		}
		
		if(grants.size()>0) {
			query.setGrants(grants);
		}
		else {
			query.setGrants(null);
		}
	}
	
}
