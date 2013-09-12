package tbrugz.sqldiff;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import javax.naming.NamingException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import tbrugz.sqldiff.datadiff.DiffSyntax;
import tbrugz.sqldiff.datadiff.ResultSetDiff;
import tbrugz.sqldiff.datadiff.SQLDataDiffSyntax;
import tbrugz.sqldump.def.DBMSResources;
import tbrugz.sqldump.def.Executor;
import tbrugz.sqldump.util.CLIProcessor;
import tbrugz.sqldump.util.CategorizedOut;
import tbrugz.sqldump.util.ConnectionUtil;
import tbrugz.sqldump.util.ParametrizedProperties;
import tbrugz.sqldump.util.Utils;

public class DiffTwoQueries implements Executor {

	static final Log log = LogFactory.getLog(DiffTwoQueries.class);

	static final String DIFF2Q = "diff2queries";
	
	public static final String PROPERTIES_FILENAME = DIFF2Q+".properties";

	static final String PREFIX = "diff2q";
	
	static final String PROP_CONNPROPPREFIX = PREFIX+".connpropprefix";
	static final String PROP_SOURCE_CONNPROPPREFIX = PREFIX+".source.connpropprefix";
	static final String PROP_TARGET_CONNPROPPREFIX = PREFIX+".target.connpropprefix";
	
	static final String PROP_SOURCEQUERY = PREFIX+".sourcesql";
	static final String PROP_TARGETQUERY = PREFIX+".targetsql";
	static final String PROP_QUERYNAME = PREFIX+".queryname";
	static final String PROP_KEYCOLS = PREFIX+".keycols";
	static final String PROP_LOOPLIMIT = PREFIX+".looplimit";
	static final String PROP_OUTPATTERN = PREFIX+".outpattern";

	static final long DEFAULT_LOOP_LIMIT = 1000L;
	static final String DEFAULT_TABLE_NAME = "<table>";
	
	final Properties prop = new ParametrizedProperties();

	boolean failonerror = true;
	
	@Override
	public void doMain(String[] args, Properties properties) throws Exception {
		if(properties!=null) {
			prop.putAll(properties);
		}
		CLIProcessor.init(DIFF2Q, args, PROPERTIES_FILENAME, prop);
		DBMSResources.instance().setup(prop);

		doIt();
	}
	
	void doIt() throws ClassNotFoundException, SQLException, NamingException, IOException {
		long initTime = System.currentTimeMillis();
		
		Connection sourceConn = null;
		Connection targetConn = null;
		
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
			Connection commonConn = ConnectionUtil.initDBConnection(commonConnPropPrefix, prop);
			if(sourceConn==null) { sourceConn = commonConn; }
			if(targetConn==null) { targetConn = commonConn; }
		}
		
		List<DiffSyntax> dss = getSyntaxes();
		
		String sourceSQL = getPropertyFailIfNull(prop, PROP_SOURCEQUERY);
		String targetSQL = getPropertyFailIfNull(prop, PROP_TARGETQUERY);
		String queryName = prop.getProperty(PROP_QUERYNAME);
		if(queryName==null) {
			log.info("null '"+PROP_QUERYNAME+"', using '"+DEFAULT_TABLE_NAME+"'");
			queryName = DEFAULT_TABLE_NAME;
		}
		List<String> keyCols = Utils.getStringListFromProp(prop, PROP_KEYCOLS, ",");
		if(keyCols==null || keyCols.size()==0 || keyCols.get(0).trim().equals("")) {
			String message = "prop '"+PROP_KEYCOLS+"' must not be null or empty";
			log.warn(message);
			throw new RuntimeException(message);
		}
		String outPattern = prop.getProperty(PROP_OUTPATTERN);
		if(outPattern==null) {
			log.info("null '"+PROP_OUTPATTERN+"', using stdout");
			outPattern = CategorizedOut.STDOUT;
		}
		
		long loopLimit = Utils.getPropLong(prop, PROP_LOOPLIMIT, DEFAULT_LOOP_LIMIT);
		
		CategorizedOut cout = new CategorizedOut(outPattern);

		PreparedStatement stmtSource = sourceConn.prepareStatement(sourceSQL);
		ResultSet rsSource = stmtSource.executeQuery();
		
		PreparedStatement stmtTarget = targetConn.prepareStatement(targetSQL);
		ResultSet rsTarget = stmtTarget.executeQuery();
		
		ResultSetDiff rsdiff = new ResultSetDiff();
		rsdiff.setLimit(loopLimit);
		rsdiff.diff(rsSource, rsTarget, queryName, keyCols, dss, cout);

		log.info("...done [elapsed="+(System.currentTimeMillis()-initTime)+"ms]");
	}

	@Override
	public void setFailOnError(boolean failonerror) {
		this.failonerror = failonerror;
	}

	public static void main(String[] args) throws Exception {
		DiffTwoQueries diff2t = new DiffTwoQueries();
		diff2t.doMain(args, diff2t.prop);
	}
	
	List<DiffSyntax> getSyntaxes() {
		//only SQLDataDiffSyntax for now...
		List<DiffSyntax> dss = new ArrayList<DiffSyntax>();
		DiffSyntax ds = new SQLDataDiffSyntax();
		ds.procProperties(prop);
		dss.add(ds);
		return dss;
	}
	
	static String getPropertyFailIfNull(Properties prop, String key) {
		String value = prop.getProperty(key);
		if(value==null) {
			throw new RuntimeException("property '"+key+"' must not be null");
		}
		return value;
	}
}
