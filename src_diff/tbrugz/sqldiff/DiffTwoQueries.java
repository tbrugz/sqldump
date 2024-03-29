package tbrugz.sqldiff;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import java.util.Properties;

import javax.naming.NamingException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import tbrugz.sqldiff.datadiff.DataDiff;
import tbrugz.sqldiff.datadiff.DiffSyntax;
import tbrugz.sqldiff.datadiff.ResultSetDiff;
import tbrugz.sqldiff.util.DiffUtil;
import tbrugz.sqldump.datadump.DataDumpUtils;
import tbrugz.sqldump.dbmd.DBMSFeatures;
import tbrugz.sqldump.def.DBMSResources;
import tbrugz.sqldump.def.Defs;
import tbrugz.sqldump.def.Executor;
import tbrugz.sqldump.util.CLIProcessor;
import tbrugz.sqldump.util.CategorizedOut;
import tbrugz.sqldump.util.ConnectionUtil;
import tbrugz.sqldump.util.ParametrizedProperties;
import tbrugz.sqldump.util.IOUtil;
import tbrugz.sqldump.util.Utils;

/*
 * XXX: add prop 'diff2q[.<env>].table/relation' - table/relation to be diffed (key cols grabbed from table metadata?)
 *  - add 'diff2q[.<env>].filter' - filter to be applyed in [each] query 
 */
public class DiffTwoQueries implements Executor {

	static final Log log = LogFactory.getLog(DiffTwoQueries.class);

	static final String DIFF2Q = "diff2queries";
	
	public static final String PROPERTIES_FILENAME = DIFF2Q+".properties";
	static final String PROPERTIES_DEFAULTS_FILENAME = "/tbrugz/sqldiff/diff2q-defaults.properties";

	public static final String PREFIX = "diff2q";
	
	static final String PROP_CONNPROPPREFIX = PREFIX+".connpropprefix";
	static final String PROP_SOURCE_CONNPROPPREFIX = PREFIX+".source.connpropprefix";
	static final String PROP_TARGET_CONNPROPPREFIX = PREFIX+".target.connpropprefix";
	
	static final String PROP_SOURCE_QUERY = PREFIX+".sourcesql";
	static final String PROP_TARGET_QUERY = PREFIX+".targetsql";
	static final String PROP_QUERY = PREFIX+".sql";
	static final String PROP_SCHEMANAME = PREFIX+".schemaname";
	static final String PROP_TABLENAME = PREFIX+".tablename";
	static final String PROP_KEYCOLS = PREFIX+".keycols";
	static final String PROP_LOOPLIMIT = PREFIX+".looplimit";
	static final String PROP_SYNTAXES = PREFIX+".syntaxes";
	static final String PROP_OUTPATTERN = PREFIX+".outpattern";
	
	static final long DEFAULT_LOOP_LIMIT = 1000L;
	static final String DEFAULT_TABLE_NAME = "_table_";
	static final String DEFAULT_SYNTAX = "SQLDataDiffSyntax";
	
	final Properties prop = new ParametrizedProperties();

	boolean failonerror = true;
	
	@Override
	public void doMain(String[] args, Properties properties) throws IOException, ClassNotFoundException, SQLException, NamingException {
		try {
			Properties propDefaults = new Properties();
			propDefaults.load(IOUtil.getResourceAsStream(PROPERTIES_DEFAULTS_FILENAME));
			prop.putAll(propDefaults);
		} catch (IOException e) {
			log.warn("Error loading properties defaults resource: "+PROPERTIES_DEFAULTS_FILENAME);
		}
		
		if(properties!=null) {
			prop.putAll(properties);
		}
		if(CLIProcessor.shouldStopExec(DIFF2Q, args)) {
			return;
		}
		CLIProcessor.init(DIFF2Q, args, PROPERTIES_FILENAME, prop);
		DBMSResources.instance().setup(prop);

		doIt();
	}
	
	void doIt() throws ClassNotFoundException, SQLException, NamingException, IOException {
		long initTime = System.currentTimeMillis();
		
		Connection sourceConn = null;
		Connection targetConn = null;
		Connection commonConn = null;
		String outPattern = null;
		
		try {
		//source
		String sourceConnPropPrefix = prop.getProperty(PROP_SOURCE_CONNPROPPREFIX);
		if(sourceConnPropPrefix!=null) {
			sourceConn = ConnectionUtil.initDBConnection(sourceConnPropPrefix, prop);
		}
		
		//target
		String targetConnPropPrefix = prop.getProperty(PROP_TARGET_CONNPROPPREFIX);
		if(targetConnPropPrefix!=null) {
			targetConn = ConnectionUtil.initDBConnection(targetConnPropPrefix, prop);
		}
		
		//"common"
		if(sourceConn==null || targetConn==null) {
			String commonConnPropPrefix = prop.getProperty(PROP_CONNPROPPREFIX);
			if(commonConnPropPrefix==null) {
				commonConnPropPrefix = PREFIX;
			}
			log.info("using common connection prop prefix '"+commonConnPropPrefix+"' for ["
					+(sourceConn==null?"source"+(targetConn==null?", target":""):(targetConn==null?"target":""))
					+"]");
			commonConn = ConnectionUtil.initDBConnection(commonConnPropPrefix, prop);
			if(sourceConn==null) { sourceConn = commonConn; }
			if(targetConn==null) { targetConn = commonConn; }
		}
		
		//DBMSResources.instance().updateMetaData(sourceConn.getMetaData(), true);
		DBMSFeatures feat = DBMSResources.instance().getSpecificFeatures(sourceConn.getMetaData());

		List<DiffSyntax> dss = getSyntaxes(feat);
		
		String sourceSQL = getPropertyFailIfNull(prop, PROP_SOURCE_QUERY, PROP_QUERY);
		String targetSQL = getPropertyFailIfNull(prop, PROP_TARGET_QUERY, PROP_QUERY);
		//XXX warn if no 'order by' present
		String tableName = prop.getProperty(PROP_TABLENAME);
		if(tableName==null) {
			log.info("null '"+PROP_TABLENAME+"', using '"+DEFAULT_TABLE_NAME+"'");
			tableName = DEFAULT_TABLE_NAME;
		}
		String schemaName = prop.getProperty(PROP_SCHEMANAME);
		List<String> keyCols = Utils.getStringListFromProp(prop, PROP_KEYCOLS, ",");
		if(keyCols==null || keyCols.size()==0 || keyCols.get(0).trim().equals("")) {
			String message = "prop '"+PROP_KEYCOLS+"' must not be null or empty";
			log.warn(message);
			//XXX: option to show prepared statement metadata (columns)
			throw new RuntimeException(message);
		}
		//XXX keyCols to upper?

		//cout
		String finalPattern = null;
		outPattern = prop.getProperty(PROP_OUTPATTERN);
		if(outPattern==null) {
			log.info("null '"+PROP_OUTPATTERN+"', using stdout");
			finalPattern = CategorizedOut.STDOUT;
		}
		else {
			finalPattern = CategorizedOut.generateFinalOutPattern(outPattern,
				Defs.addSquareBraquets(Defs.PATTERN_SCHEMANAME), 
				Defs.addSquareBraquets(Defs.PATTERN_TABLENAME),
				Defs.addSquareBraquets(Defs.PATTERN_CHANGETYPE),
				Defs.addSquareBraquets(DataDiff.PATTERN_FILEEXT));
		}
		//CategorizedOut cout = new CategorizedOut(finalPattern);
		
		long loopLimit = Utils.getPropLong(prop, PROP_LOOPLIMIT, DEFAULT_LOOP_LIMIT);
		
		PreparedStatement stmtSource = sourceConn.prepareStatement(sourceSQL);
		ResultSet rsSource = stmtSource.executeQuery();
		
		PreparedStatement stmtTarget = targetConn.prepareStatement(targetSQL);
		ResultSet rsTarget = stmtTarget.executeQuery();
		
		ResultSetDiff rsdiff = new ResultSetDiff();
		rsdiff.setLimit(loopLimit);
		try {
			log.info("starting diff...");
			rsdiff.diff(rsSource, rsTarget, schemaName, tableName, keyCols, dss, finalPattern);
		}
		catch(IllegalArgumentException e) {
			log.warn("error diffing: "+e);
			log.info("source-sql columns: "+DataDumpUtils.getColumnNames(rsSource.getMetaData()));
			log.info("target-sql columns: "+DataDumpUtils.getColumnNames(rsSource.getMetaData()));
			throw e;
		}
		}
		finally {
			ConnectionUtil.closeConnection(sourceConn);
			ConnectionUtil.closeConnection(targetConn);
			ConnectionUtil.closeConnection(commonConn);
		}

		log.info("...done [elapsed="+(System.currentTimeMillis()-initTime)+"ms] [outpattern: "+outPattern+"]");
	}

	@Override
	public void setFailOnError(boolean failonerror) {
		this.failonerror = failonerror;
	}

	public static void main(String[] args) throws ClassNotFoundException, IOException, SQLException, NamingException {
		DiffTwoQueries diff2t = new DiffTwoQueries();
		diff2t.doMain(args, null);
	}
	
	List<DiffSyntax> getSyntaxes(DBMSFeatures feat) {
		List<DiffSyntax> dss = DiffUtil.getSyntaxes(prop, feat, PROP_SYNTAXES);
		if(dss.size()==0) {
			dss.add(DiffUtil.getSyntax(prop, feat, DEFAULT_SYNTAX));
		}
		return dss;
	}
	
	static String getPropertyFailIfNull(Properties prop, String key, String altKey) {
		String value = prop.getProperty(key);
		if(value==null && altKey!=null) {
			value = prop.getProperty(altKey);
		}
		if(value==null) {
			throw new RuntimeException("property '"+key+"'"+(altKey!=null?" (or '"+altKey+"')":"")+" must not be null");
		}
		return value;
	}
}
