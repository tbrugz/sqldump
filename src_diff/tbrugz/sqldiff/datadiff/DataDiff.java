package tbrugz.sqldiff.datadiff;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.TreeSet;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import tbrugz.sqldiff.SQLDiff;
import tbrugz.sqldump.datadump.DataDump;
import tbrugz.sqldump.datadump.DataDumpUtils;
import tbrugz.sqldump.dbmodel.Column;
import tbrugz.sqldump.dbmodel.Constraint;
import tbrugz.sqldump.dbmodel.SchemaModel;
import tbrugz.sqldump.dbmodel.Table;
import tbrugz.sqldump.def.AbstractFailable;
import tbrugz.sqldump.def.DBMSResources;
import tbrugz.sqldump.def.Defs;
import tbrugz.sqldump.def.ProcessingException;
import tbrugz.sqldump.resultset.ResultSetColumnMetaData;
import tbrugz.sqldump.sqlrun.SQLStmtScanner;
import tbrugz.sqldump.util.CategorizedOut;
import tbrugz.sqldump.util.ConnectionUtil;
import tbrugz.sqldump.util.StringDecorator;
import tbrugz.sqldump.util.Utils;

public class DataDiff extends AbstractFailable {

	static final Log log = LogFactory.getLog(DataDiff.class);
	
	public static final String PROP_DATADIFF_TABLES = SQLDiff.PROP_PREFIX+".datadiff.tables";
	public static final String PROP_DATADIFF_IGNORETABLES = SQLDiff.PROP_PREFIX+".datadiff.ignoretables";
	public static final String PROP_DATADIFF_OUTFILEPATTERN = SQLDiff.PROP_PREFIX+".datadiff.outfilepattern";
	public static final String PROP_DATADIFF_LOOPLIMIT = SQLDiff.PROP_PREFIX+".datadiff.looplimit";
	public static final String PROP_DATADIFF_IMPORTCHARSET = SQLDiff.PROP_PREFIX+".datadiff.importcharset";
	public static final String PROP_DATADIFF_USECOMMONCOLUMNS = SQLDiff.PROP_PREFIX+".datadiff.usecommoncolumns";
	
	StringDecorator quoteAllDecorator;
	
	Properties prop = null;
	List<String> tablesToDiffFilter = new ArrayList<String>();
	List<String> tablesToIgnore = new ArrayList<String>();
	String outFilePattern = null;
	long loopLimit = 0;
	boolean applyDataDiff = false;
	boolean useCommonColumns = false;
	boolean orderWithAllColumnsIfNoUK = true; //XXX add property for 'orderWithAllColumnsIfNoUK'
	
	SchemaModel sourceSchemaModel = null;
	SchemaModel targetSchemaModel = null;
	Connection sourceConn = null;
	Connection targetConn = null;
	String sourceId;
	String targetId;
	
	String importCharset;
	
	public void setProperties(Properties prop) {
		tablesToDiffFilter = Utils.getStringListFromProp(prop, PROP_DATADIFF_TABLES, ",");
		tablesToIgnore = Utils.getStringListFromProp(prop, PROP_DATADIFF_IGNORETABLES, ",");
		String quote = DBMSResources.instance().getIdentifierQuoteString();
		quoteAllDecorator = new StringDecorator.StringQuoterDecorator(quote);
		outFilePattern = prop.getProperty(PROP_DATADIFF_OUTFILEPATTERN);
		loopLimit = Utils.getPropLong(prop, PROP_DATADIFF_LOOPLIMIT, loopLimit);

		sourceId = prop.getProperty(SQLDiff.PROP_SOURCE);
		targetId = prop.getProperty(SQLDiff.PROP_TARGET);
		log.debug("source: "+sourceId+" ; target: "+targetId);
		importCharset = prop.getProperty(PROP_DATADIFF_IMPORTCHARSET, DataDumpUtils.CHARSET_UTF8);
		useCommonColumns = Utils.getPropBool(prop, PROP_DATADIFF_USECOMMONCOLUMNS, useCommonColumns);
		
		this.prop = prop;
	}

	public void setFailOnError(boolean failonerror) {
		this.failonerror = failonerror;
	}
	
	public void setSourceSchemaModel(SchemaModel schemamodel) {
		sourceSchemaModel = schemamodel;
	}

	public void setTargetSchemaModel(SchemaModel schemamodel) {
		targetSchemaModel = schemamodel;
	}

	/*
	public void setSchemaDiff(SchemaDiff schemadiff) {
	}

	public void setSourceGrabber(SchemaModelGrabber grabber) {
	}

	public void setTargetGrabber(SchemaModelGrabber grabber) {
	}
	*/

	public void setSourceConnection(Connection conn) {
		sourceConn = conn;
	}

	public void setTargetConnection(Connection conn) {
		targetConn = conn;
	}

	public void process() throws SQLException, IOException {
		if(sourceSchemaModel==null || targetSchemaModel==null) {
			log.error("can't datadiff if source or taget models are null");
			if(failonerror) { throw new ProcessingException("can't datadiff if source or taget models are null"); }
			return;
		}
		
		Set<Table> targetTables = targetSchemaModel.getTables();
		Set<Table> sourceTables = sourceSchemaModel.getTables();
		
		Set<Table> tablesToDiff = new TreeSet<Table>();
		tablesToDiff.addAll(targetTables);
		tablesToDiff.retainAll(sourceTables); //only diff tables contained in source & target models...
		
		Map<String, Table> sourceTablesMap = new HashMap<String, Table>();
		for(Table t: sourceTables) {
			sourceTablesMap.put(t.getName(), t);
		}
		
		boolean
			sourceConnCreated = false,
			targetConnCreated = false,
			sourceMustImportData = false,
			targetMustImportData = false;
		
		//~XXX: option to create resultset based on file (use csv/regex importer RSdecorator) ?
		//XXXxx: insert into temporary table - H2 database by default?
		if(sourceConn==null) {
			sourceConn = getConn(sourceId);
			if(sourceConn==null) { return; }
			sourceConnCreated = true;
			sourceMustImportData = mustImportData(sourceId);
		}
		if(targetConn==null) {
			targetConn = getConn(targetId);
			if(targetConn==null) { return; }
			targetConnCreated = true;
			targetMustImportData = mustImportData(targetId);
		}
		
		ResultSetDiff rsdiff = new ResultSetDiff();
		rsdiff.setLimit(loopLimit);
		
		CategorizedOut cout = getCOut();
		
		if(sourceMustImportData) {
			log.info("[source] importing data for datadiff from: "+getImportDataPattern(sourceId));
		}
		if(targetMustImportData) {
			log.info("[target] importing data for datadiff from: "+getImportDataPattern(targetId));
		}

		List<DiffSyntax> dss = getSyntaxes(prop, applyDataDiff);
		if(dss.size()==0) {
			String message = "no datadiff syntaxes defined: no table's data will be diffed..."; 
			log.error(message);
			if(failonerror) { throw new ProcessingException(message); }
		}
		else {
		
		int tablesDiffedCount = 0;
		int tablesToDiffCount = 0;
		for(Table table: tablesToDiff) {
			if(tablesToDiffFilter!=null) {
				if(tablesToDiffFilter.contains(table.getName())) {
					tablesToDiffFilter.remove(table.getName());
				}
				else {
					continue;
				}
			}
			if(tablesToIgnore!=null) {
				if(tablesToIgnore.contains(table.getName())) {
					tablesToIgnore.remove(table.getName());
					log.info("ignoring table '"+table+"'");
					continue;
				}
			}
			
			tablesToDiffCount++;

			String columnsForSelect = null;
			//XXX select '*' or all column names? option to select by property?
			if(useCommonColumns) {
				List<Column> cols = getCommonColumns(table, sourceTablesMap.get(table.getName()));
				if(cols.size()==0) {
					log.warn("no common columns ["+table+"] for datadiff");
					continue;
				}
				if(table.getColumns().size()>cols.size()) {
					log.info("tablediff ["+table+"] using "+cols.size()+" column [of "+table.getColumns().size()+"]");
				}
				columnsForSelect = getColumnsForSelect(cols);
			}
			else {
				//replaced '*' for column names - same column order for both resultsets
				columnsForSelect = getColumnsForSelect(table);
			}

			List<String> keyCols = null;
			Constraint ctt = table.getPKConstraint();
			if(ctt!=null) {
				keyCols = ctt.getUniqueColumns();
			}
			if(keyCols==null) {
				if(orderWithAllColumnsIfNoUK) {
					log.info("table '"+table+"' has no PK. using all columns in 'order by'");
					keyCols = Utils.splitStringWithTrim(columnsForSelect, ",");
				}
				else {
					log.warn("table '"+table+"' has no PK. diff disabled");
					continue;
				}
			}
			
			String sql = DataDump.getQuery(table, columnsForSelect, null, null, true);

			ResultSet rsSource = null, rsTarget=null;
			
			if(sourceMustImportData) {
				importData(table, sourceConn, sourceId);
			}
			try {
				PreparedStatement stmtSource = sourceConn.prepareStatement(sql);
				rsSource = stmtSource.executeQuery();
			}
			catch(SQLException e) {
				log.warn("error in sql exec [source ; '"+table+"']: "+sql);
				continue;
			}
			
			if(targetMustImportData) {
				importData(table, targetConn, targetId);
			}
			try {
				PreparedStatement stmtTarget = targetConn.prepareStatement(sql);
				rsTarget = stmtTarget.executeQuery();
			}
			catch(SQLException e) {
				log.warn("error in sql exec [target]: "+sql);
				continue;
			}
			
			//TODOne: check if rsmetadata is equal between RSs...
			ResultSetColumnMetaData sRSColmd = new ResultSetColumnMetaData(rsSource.getMetaData()); 
			ResultSetColumnMetaData tRSColmd = new ResultSetColumnMetaData(rsTarget.getMetaData());
			if(!sRSColmd.equals(tRSColmd)) {
				log.warn("["+table+"] metadata from ResultSets differ. diff disabled");
				log.debug("["+table+"] diff:\nsource: "+sRSColmd+" ;\ntarget: "+tRSColmd);
				continue;
			}
			
			log.debug("diff for table '"+table+"'...");
			rsdiff.diff(rsSource, rsTarget, table.getName(), keyCols, dss, cout);
			log.info("table '"+table+"' data diff: "+rsdiff.getStats());
			
			rsSource.close(); rsTarget.close();
			
			tablesDiffedCount++;
			//XXX: drop table if imported?
		}
		if(tablesToDiffFilter!=null && tablesToDiffFilter.size()>0) {
			log.warn("tables not found for diff: "+Utils.join(tablesToDiffFilter, ", "));
			//XXX: log which tables do not exist in source or target models
		}
		if(tablesToIgnore!=null && tablesToIgnore.size()>0) {
			log.warn("tables to ignore that were not found: "+Utils.join(tablesToIgnore, ", "));
		}
		log.info(tablesDiffedCount+" [of "+tablesToDiffCount+"] tables diffed");
		
		}

		if(sourceConnCreated) {
			ConnectionUtil.closeConnection(sourceConn);
		}
		if(targetConnCreated) {
			ConnectionUtil.closeConnection(targetConn);
		}
	}
	
	String getColumnsForSelect(Table t) {
		return Utils.join(t.getColumnNames(), ", ");
	}

	String getColumnsForSelect(List<Column> cols) {
		StringBuilder sb = new StringBuilder();
		for(int i=0;i<cols.size();i++) {
			if(i>0) { sb.append(", "); }
			Column c = cols.get(i);
			sb.append(c.getName());
		}
		return sb.toString();
	}
	
	List<Column> getCommonColumns(Table t1, Table t2) {
		List<Column> cols = new ArrayList<Column>();
		for(Column c: t1.getColumns()) {
			if(t2.getColumn(c.getName())!=null) {
				cols.add(c);
			}
		}
		return cols;
	}
	
	static List<DiffSyntax> getSyntaxes(Properties prop, boolean applyDataDiff) throws SQLException {
		List<DiffSyntax> dss = new ArrayList<DiffSyntax>();
		List<String> dsStrs = new ArrayList<String>();
		//FIXedME: select DiffSyntax (based on properties?)
		// maybe add SQLDataDiffSyntax only if 'sqldiff.datadiff.outfilepattern' is set?
		if(prop.getProperty(PROP_DATADIFF_OUTFILEPATTERN)!=null) {
			DiffSyntax ds = new SQLDataDiffSyntax();
			ds.procProperties(prop);
			dss.add(ds);
			dsStrs.add(ds.getClass().getSimpleName());
		}
		if(applyDataDiff) {
			DiffSyntax sdd2db = new SQLDataDiffToDBSyntax();
			sdd2db.procProperties(prop);
			dss.add(sdd2db);
			dsStrs.add(sdd2db.getClass().getSimpleName());
		}
		
		if(dss.size()>0) {
			log.info("datadiff syntaxes: "+dsStrs);
		}
		return dss;
	}
	
	CategorizedOut getCOut() {
		if(outFilePattern==null) {
			log.warn("outFilePattern is null (prop '"+PROP_DATADIFF_OUTFILEPATTERN+"' not defined?) - using 'null' writer"); //XXX should be 'warn' level?
			//if(failonerror) { throw new ProcessingException("outFilePattern is null (prop '"+PROP_DATADIFF_OUTFILEPATTERN+"' not defined?)"); }
			return new CategorizedOut(CategorizedOut.NULL_WRITER);
		}
		
		log.info("outfilepattern: "+new File(outFilePattern).getAbsolutePath());
		
		String finalPattern = CategorizedOut.generateFinalOutPattern(outFilePattern,
				Defs.addSquareBraquets(Defs.PATTERN_SCHEMANAME), 
				Defs.addSquareBraquets(Defs.PATTERN_TABLENAME),
				Defs.addSquareBraquets(Defs.PATTERN_CHANGETYPE));
		return new CategorizedOut(finalPattern);
	}
	
	Connection getConn(String grabberId) {
		try {
			if(mustImportData(grabberId)) {
				Connection conn =  ConnectionUtil.initDBConnection("", getTempDBConnProperties(grabberId));
				log.info("new connection [grabberId="+grabberId+"]: "+conn);
				return conn;
			}
			else {
				String propsPrefix = "sqldiff."+grabberId;
				if(ConnectionUtil.isBasePropertiesDefined(propsPrefix, prop)) {
					Connection conn = ConnectionUtil.initDBConnection(propsPrefix, prop);
					log.info("database connection created [grabberId="+grabberId+"]");
					return conn;
				}
				log.error("base connection properties not defined, can't proceed [grabberId="+grabberId+"]");
				log.info("base properties are: "+ConnectionUtil.getBasePropertiesSuffixStr());
				if(failonerror) { throw new ProcessingException("base connection properties not defined, can't proceed [grabberId="+grabberId+"]"); }
				return null;
			}
		} catch (Exception e) {
			log.error("error creating connection [grabberId="+grabberId+"]: "+e);
			log.debug("error creating connection",e);
			if(failonerror) { throw new ProcessingException("error creating connection [grabberId="+grabberId+"]: "+e,e); }
			return null;
		}
	}
	
	String getImportDataPattern(String grabberId) {
		String inDataFilePatternProp = "sqldiff.datadiff."+grabberId+".indatafilepattern";
		return prop.getProperty(inDataFilePatternProp);
	}
	
	boolean mustImportData(String grabberId) {
		return getImportDataPattern(grabberId)!=null;
	}
	
	static final String SCHEMANAME_PATTERN = Pattern.quote(Defs.addSquareBraquets(Defs.PATTERN_SCHEMANAME));
	static final String TABLENAME_PATTERN = Pattern.quote(Defs.addSquareBraquets(Defs.PATTERN_TABLENAME));
	
	void importData(Table table, Connection conn, String grabberId) throws SQLException, FileNotFoundException {
		Statement st = conn.createStatement();
		
		//create schema
		if(table.getSchemaName()!=null) {
			st.executeUpdate("create schema if not exists "+table.getSchemaName());
			st.executeUpdate("set schema "+table.getSchemaName());
		}
		
		//create table
		st.executeUpdate(table.getDefinition(true));
		
		//import data
		String dataPattern = getImportDataPattern(grabberId);
		//log.info("importing data for datadiff from: "+dataPattern);
		String fileName = dataPattern
				.replaceAll(SCHEMANAME_PATTERN, Matcher.quoteReplacement(table.getSchemaName()) )
				.replaceAll(TABLENAME_PATTERN, Matcher.quoteReplacement(table.getName()) );
		File file = new File(fileName);
		log.debug("importing data from file: "+file);
		SQLStmtScanner scanner = new SQLStmtScanner(file, importCharset, false);
		long updateCount = 0;
		for(String sql: scanner) {
			updateCount += st.executeUpdate(sql);
		}
		log.info("imported "+updateCount+" rows into table '"+table.getQualifiedName()+"' from file '"+file+"'");
		//log.info("imported "+updateCount+" rows");
	}
	
	static String[][] defaultTempDbProp = {
		{".driverclass", "org.h2.Driver"},
		{".dburl", "jdbc:h2:mem:"}
	};
	
	static Properties getTempDBConnProperties(String grabberId) {
		Properties prop = new Properties();
		for(String[] sarr: defaultTempDbProp) {
			prop.put(sarr[0], sarr[1]);
		}
		return prop;
	}

	public boolean isApplyDataDiff() {
		return applyDataDiff;
	}

	public void setApplyDataDiff(boolean applyDataDiff) {
		this.applyDataDiff = applyDataDiff;
	}
	
}
