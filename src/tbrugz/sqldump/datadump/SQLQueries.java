package tbrugz.sqldump.datadump;

import java.sql.ParameterMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
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
	
	protected static final String PROP_QUERIES = "sqldump.queries";
	protected static final String PROP_QUERIES_RUN = PROP_QUERIES+".runqueries";
	protected static final String PROP_QUERIES_ADD_TO_MODEL = PROP_QUERIES+".addtomodel";
	protected static final String PROP_QUERIES_SCHEMA = PROP_QUERIES+".schemaname";
	protected static final String PROP_QUERIES_GRABCOLSINFOFROMMETADATA = PROP_QUERIES+".grabcolsinfofrommetadata";

	protected static final String DEFAULT_QUERIES_SCHEMA = "SQLQUERY"; //XXX: default schema to be current schema for dumping?
	
	protected static final String ROLES_DELIMITER_STR = "|";
	protected static final String ROLES_DELIMITER = Pattern.quote(ROLES_DELIMITER_STR);
	
	static final Log log = LogFactory.getLog(SQLQueries.class);
	
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
		
		String queriesStr = prop.getProperty(PROP_QUERIES);
		if(queriesStr==null) {
			log.error("prop '"+PROP_QUERIES+"' not defined");
			if(failonerror) {
				throw new ProcessingException("SQLQueries: prop '"+PROP_QUERIES+"' not defined");
			}
			return;
		}
		String[] queriesArr = queriesStr.split(",");
		int i=0;
		int queriesGrabbed=0;
		//List<Query> queries = new ArrayList<Query>(); 
		for(String qid: queriesArr) {
			qid = qid.trim();
			
			String queryName = prop.getProperty("sqldump.query."+qid+".name");
			DBMSFeatures feat = DBMSResources.instance().getSpecificFeatures(model.getSqlDialect());
			List<DumpSyntax> syntaxList = getQuerySyntaxes(qid, feat);
			if(runQueries && syntaxList==null) {
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
			if(sql==null) {
				log.warn("no SQL defined for query [id="+qid+";propkey='"+"sqldump.query."+qid+".sql(file)"+"']");
				continue;
			}
			
			PreparedStatement stmt = null;
			try {
				stmt = conn.prepareStatement(sql);
			} catch (SQLException e) {
				String message = "error creating prepared statement [id="+qid+";sql="+sql+"]: "+e.getMessage();
				log.warn(message);
				if(failonerror) {
					throw new ProcessingException(message, e);
				}
				//continue; //?
			}
			
			if(queryName==null) {
				log.info("no name defined for query [id="+qid+"] (query name will be equal to id)");
				queryName = qid;
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
				log.info("partitionby-patterns[id="+qid+"]: "+partitionsBy); //XXX: move log into DataDump?
			}

			List<String> keyCols = Utils.getStringListFromProp(prop, "sqldump.query."+qid+".keycols", ",");
			
			ResultSetDecoratorFactory rsdf = null;
			String rsDecoratorFactory = prop.getProperty("sqldump.query."+qid+".rsdecoratorfactory");
			String rsArgPrepend = "sqldump.query."+qid+".rsdecorator.";
			List<String> rsFactoryArgs = Utils.getKeysStartingWith(prop, rsArgPrepend);
			if(rsDecoratorFactory!=null) {
				rsdf = (ResultSetDecoratorFactory) Utils.getClassInstance(rsDecoratorFactory, "tbrugz.sqldump.resultset", null);
				for(String arg: rsFactoryArgs) {
					rsdf.set(arg.substring(rsArgPrepend.length()), prop.getProperty(arg));
				}
			}

			// adding query to model
			if(addQueriesToModel) {
				String remarks = prop.getProperty("sqldump.query."+qid+".remarks");
				String roles = prop.getProperty("sqldump.query."+qid+".roles");
				queriesGrabbed += addQueryToModelInternal(qid, queryName, defaultSchemaName, stmt, sql, keyCols, params, remarks, roles, rsDecoratorFactory, rsFactoryArgs, rsArgPrepend);
			}
			
			if(runQueries && stmt!=null) {
				try {
					log.debug("running query [id="+qid+"; name="+queryName+"]: "+sql);
					DataDump dd = new DataDump();
					dd.runQuery(conn, stmt, params, prop, defaultSchemaName, qid, queryName, charset, rowlimit, syntaxList, 
							partitionsBy!=null ? partitionsBy.toArray(new String[]{}) : null, 
							keyCols, null, null, rsdf);
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
			log.info(i+" queries runned");
		}
		if(addQueriesToModel) {
			log.info(queriesGrabbed+" queries grabbed from properties");
		}
		if(!runQueries && !addQueriesToModel) {
			log.warn("no queries runned or grabbed");
		}
	}
	
	List<DumpSyntax> getQuerySyntaxes(String qid, DBMSFeatures feat) {
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
					if(ds.needsDBMSFeatures()) { ds.setFeatures(feat); }
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
	
	int addQueryToModelInternal(String qid, String queryName, String defaultSchemaName,
			PreparedStatement stmt, String sql, List<String> keyCols,
			List<String> params, String remarks, String roles,
			String rsDecoratorFactory, List<String> rsFactoryArgs, String rsArgPrepend) {
		
		String schemaName = prop.getProperty("sqldump.query."+qid+".schemaname", defaultSchemaName);
		String colNames = prop.getProperty("sqldump.query."+qid+".cols");
		boolean grabInfoFromMetadata = Utils.getPropBool(prop, PROP_QUERIES_GRABCOLSINFOFROMMETADATA, false);
		
		//XXX: add prop for 'addAlsoAsTable'? default is false
		return addQueryToModel(qid, queryName, schemaName, colNames, grabInfoFromMetadata, /*addAlsoAsTable*/ false, stmt, sql, keyCols, params, remarks, roles, rsDecoratorFactory, rsFactoryArgs, rsArgPrepend);
	}
	
	public int addQueryToModel(String qid, String queryName, String schemaName,
			String colNames, boolean grabInfoFromMetadata, boolean addAlsoAsTable,
			PreparedStatement stmt, String sql, List<String> keyCols,
			List<String> params, String remarks, String roles,
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
			if(grabInfoFromMetadata) {
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
					query.setColumns(new ArrayList<Column>());
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
					query.setParameterCount(null);
					log.warn("parameter metadata's sqlexception: "+e.toString().trim());
					log.debug("parameter metadata's sqlexception: "+e.getMessage(), e);
				}
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
