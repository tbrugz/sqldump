package tbrugz.sqldiff.datadiff;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import tbrugz.sqldiff.SQLDiff;
import tbrugz.sqldump.datadump.DataDump;
import tbrugz.sqldump.datadump.DataDumpUtils;
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
import tbrugz.sqldump.util.SQLUtils;
import tbrugz.sqldump.util.StringDecorator;
import tbrugz.sqldump.util.Utils;

public class DataDiff extends AbstractFailable {

	static final Log log = LogFactory.getLog(DataDiff.class);
	
	public static final String PROP_DATADIFF_TABLES = SQLDiff.PROP_PREFIX+".datadiff.tables";
	public static final String PROP_DATADIFF_IGNORETABLES = SQLDiff.PROP_PREFIX+".datadiff.ignoretables";
	public static final String PROP_DATADIFF_OUTFILEPATTERN = SQLDiff.PROP_PREFIX+".datadiff.outfilepattern";
	public static final String PROP_DATADIFF_LOOPLIMIT = SQLDiff.PROP_PREFIX+".datadiff.looplimit";
	public static final String PROP_DATADIFF_IMPORTCHARSET = SQLDiff.PROP_PREFIX+".datadiff.importcharset";
	
	StringDecorator quoteAllDecorator;
	
	Properties prop = null;
	List<String> tablesToDiffFilter = new ArrayList<String>();
	List<String> tablesToIgnore = new ArrayList<String>();
	String outFilePattern = null;
	long loopLimit = 0;
	
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
		
		Set<Table> tablesToDiff = sourceSchemaModel.getTables();
		Set<Table> targetTables = targetSchemaModel.getTables();
		tablesToDiff.retainAll(targetTables);
		
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
		
		if(outFilePattern==null) {
			log.error("outFilePattern is null (prop '"+PROP_DATADIFF_OUTFILEPATTERN+"' not defined?)");
			if(failonerror) { throw new ProcessingException("outFilePattern is null (prop '"+PROP_DATADIFF_OUTFILEPATTERN+"' not defined?)"); }
			return;
		}
		
		log.info("outfilepattern: "+new File(outFilePattern).getAbsolutePath());
		
		String finalPattern = CategorizedOut.generateFinalOutPattern(outFilePattern,
				Defs.addSquareBraquets(Defs.PATTERN_SCHEMANAME), 
				Defs.addSquareBraquets(Defs.PATTERN_TABLENAME),
				Defs.addSquareBraquets(Defs.PATTERN_CHANGETYPE));
		CategorizedOut cout = new CategorizedOut(finalPattern);
		
		if(sourceMustImportData) {
			log.info("[source] importing data for datadiff from: "+getImportDataPattern(sourceId));
		}
		if(targetMustImportData) {
			log.info("[target] importing data for datadiff from: "+getImportDataPattern(targetId));
		}
		
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

			List<String> keyCols = null;
			Constraint ctt = table.getPKConstraint();
			if(ctt!=null) {
				keyCols = ctt.uniqueColumns;
			}
			if(keyCols==null) {
				log.warn("table '"+table+"' has no PK. diff disabled");
				continue;
			}
			
			String sql = DataDump.getQuery(table, "*", null, null, true);
			
			if(sourceMustImportData) {
				importData(table, sourceConn, sourceId);
			}
			Statement stmtSource = sourceConn.createStatement();
			ResultSet rsSource = stmtSource.executeQuery(sql);
			
			if(targetMustImportData) {
				importData(table, targetConn, targetId);
			}
			Statement stmtTarget = targetConn.createStatement();
			ResultSet rsTarget = stmtTarget.executeQuery(sql);
			
			//TODOne: check if rsmetadata is equal between RSs...
			ResultSetColumnMetaData sRSColmd = new ResultSetColumnMetaData(rsSource.getMetaData()); 
			ResultSetColumnMetaData tRSColmd = new ResultSetColumnMetaData(rsTarget.getMetaData());
			if(!sRSColmd.equals(tRSColmd)) {
				log.warn("["+table+"] metadata from ResultSets differ. diff disabled");
				log.debug("["+table+"] diff:\nsource: "+sRSColmd+" ;\ntarget: "+tRSColmd);
				continue;
			}
			
			log.debug("diff for table '"+table+"'...");
			DiffSyntax ds = getSyntax(prop);
			rsdiff.diff(rsSource, rsTarget, table.getName(), keyCols, ds, cout);
			log.info("table '"+table+"' data diff: "+rsdiff.getStats());
			
			rsSource.close(); rsTarget.close();
			
			//XXX: drop table if imported?
		}
		
		if(tablesToDiffFilter!=null && tablesToDiffFilter.size()>0) {
			log.warn("tables not found for diff: "+Utils.join(tablesToDiffFilter, ", "));
		}

		if(tablesToIgnore!=null && tablesToIgnore.size()>0) {
			log.warn("tables to ignore that were not found: "+Utils.join(tablesToIgnore, ", "));
		}

		if(sourceConnCreated) {
			SQLUtils.ConnectionUtil.closeConnection(sourceConn);
		}
		if(targetConnCreated) {
			SQLUtils.ConnectionUtil.closeConnection(targetConn);
		}
	}
	
	static DiffSyntax getSyntax(Properties prop) throws SQLException {
		DiffSyntax ds = new SQLDataDiffSyntax(); //XXX: option/prop to select DiffSyntax (based on properties?)?
		ds.procProperties(prop);
		return ds;
	}
	
	Connection getConn(String grabberId) {
		try {
			if(mustImportData(grabberId)) {
				Connection conn =  SQLUtils.ConnectionUtil.initDBConnection("", getTempDBConnProperties(grabberId));
				log.info("new connection [grabberId="+grabberId+"]: "+conn);
				return conn;
			}
			else {
				String propsPrefix = "sqldiff."+grabberId;
				if(SQLUtils.ConnectionUtil.isBasePropertiesDefined(propsPrefix, prop)) {
					Connection conn = SQLUtils.ConnectionUtil.initDBConnection(propsPrefix, prop);
					log.info("database connection created [grabberId="+grabberId+"]");
					return conn;
				}
			}
			
			log.error("base connection properties not defined, can't proceed [grabberId="+grabberId+"]");
			if(failonerror) { throw new ProcessingException("base connection properties not defined, can't proceed [grabberId="+grabberId+"]"); }
			return null;
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
	
	static final String SCEHMANAME_PATTERN = Pattern.quote(Defs.addSquareBraquets(Defs.PATTERN_SCHEMANAME));
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
				.replaceAll(SCEHMANAME_PATTERN, Matcher.quoteReplacement(table.getSchemaName()) )
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
	
}
