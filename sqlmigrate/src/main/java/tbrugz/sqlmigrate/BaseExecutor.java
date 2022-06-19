package tbrugz.sqlmigrate;

import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.Properties;

import javax.naming.NamingException;

import org.apache.commons.logging.Log;

import tbrugz.sqldump.dbmodel.Column.ColTypeUtil;
import tbrugz.sqldump.def.DBMSResources;
import tbrugz.sqldump.def.ProcessingException;
import tbrugz.sqldump.util.CLIProcessor;
import tbrugz.sqldump.util.ConnectionUtil;
import tbrugz.sqldump.util.ParametrizedProperties;
import tbrugz.sqldump.util.Utils;

public abstract class BaseExecutor implements tbrugz.sqldump.def.Executor {

	//static final String PROPERTIES_FILENAME = "sqlmigrate.properties";
	//static final String CONN_PROPS_PREFIX = "sqlmigrate";
	//static final String PRODUCT_NAME = "sqlmigrate";

	Connection conn;
	boolean failonerror = true;
	boolean dryRun = false;
	final Properties papp = new ParametrizedProperties();
	
	public abstract Log getLogger();
	//public abstract String getConnectionPropsPrefix();
	public abstract String getPropertiesPrefix();
	public abstract String getProductName();
	public abstract String getPropertiesFilename();
	
	@Override
	public void doMain(String[] args, Properties p) throws ClassNotFoundException, IOException, SQLException, NamingException, IllegalStateException {
		doMain(args, p, null);
	}
	
	public void doMain(String[] args, Properties p, Connection c) throws ClassNotFoundException, IOException, SQLException, NamingException, IllegalStateException {
		if(CLIProcessor.shouldStopExec(getProductName(), args)) {
			return;
		}
		try {
			if(p!=null) {
				papp.putAll(p);
				if(args!=null) {
					String message = "args informed "+Arrays.asList(args)+" but won't be processed";
					getLogger().warn(message);
					//if(failonerror) { //always true?
					throw new IllegalStateException(message);
					//}
				}
			}
			else {
				CLIProcessor.init(getProductName(), args, getPropertiesFilename(), papp);
			}
			init(c);
			if(conn==null) { return; }
			doIt();
		}
		finally {
			end(c==null);
		}
	}

	void init(Connection c) throws IOException, ClassNotFoundException, SQLException, NamingException {
		ColTypeUtil.setProperties(papp);
		
		// subclasses properties
		procProterties();
		// default properties
		// dryRun: -n , --dry-run, xxx.dry-run
		dryRun = Utils.getPropBool(papp, getProductName() + ".dry-run", dryRun);
		failonerror = Utils.getPropBool(papp, getPropertiesPrefix() + ".failonerror", failonerror);

		//commitStrategy = getCommitStrategy( papp.getProperty(PROP_COMMIT_STATEGY), commitStrategy );
		//boolean commitStrategyIsAutocommit = commitStrategy==CommitStrategy.AUTO_COMMIT;
		if(c!=null) {
			conn = c;
			/*boolean isAutocommit = c.getAutoCommit();
			if(isAutocommit != commitStrategyIsAutocommit) {
				c.setAutoCommit(commitStrategyIsAutocommit);
			}*/
		}
		else {
			String connPrefix = papp.getProperty(getPropertiesPrefix() + ".connpropprefix");
			if(connPrefix==null) {
				connPrefix = getPropertiesPrefix();
			}
			// using autocommit property (default is false)
			boolean isAutoCommit = Utils.getPropBool(papp, connPrefix+ConnectionUtil.SUFFIX_AUTOCOMMIT, false);
			// if connPrefix+".autocommit" is set, show warning
			/*{
				String autocommitPropKey = connPrefix+ConnectionUtil.SUFFIX_AUTOCOMMIT;
				String autocommitPropValue = papp.getProperty(autocommitPropKey);
				if(autocommitPropValue != null) {
					getLogger().warn("prop '"+autocommitPropKey+"' (value: "+autocommitPropValue+") will be ignored. Commit Strategy is "+commitStrategy);
				}
			}*/
			//conn = ConnectionUtil.initDBConnection(connPrefix, papp, commitStrategyIsAutocommit);
			conn = ConnectionUtil.initDBConnection(connPrefix, papp, isAutoCommit, isDryRun());
			if(conn==null) {
				throw new ProcessingException("null connection [prop prefix: '"+connPrefix+"']");
			}
		}

		ConnectionUtil.showDBInfo(conn.getMetaData());

		//inits DBMSResources
		DBMSResources.instance().setup(papp);
		//DBMSResources.instance().updateMetaData(conn.getMetaData()); //XXX: really needed?
		
		//inits specific DBMSFeatures class
		//XXX: really needed?
		//DBMSFeatures feats = DBMSResources.instance().databaseSpecificFeaturesClass();
		//log.debug("DBMSFeatures: "+feats);
		//defaultEncoding = papp.getProperty(Constants.SQLRUN_PROPS_PREFIX+Constants.SUFFIX_DEFAULT_ENCODING, DataDumpUtils.CHARSET_UTF8);
		//jmxCreateMBean = Utils.getPropBool(papp, PROP_JMX_CREATE_MBEAN, jmxCreateMBean);
	}
	
	protected abstract void procProterties();
	
	//protected abstract void init(Connection c);
	protected abstract void doIt();
	
	public boolean isFailOnError() {
		return this.failonerror;
	}
	
	@Override
	public void setFailOnError(boolean failonerror) {
		this.failonerror = failonerror;
	}

	public boolean isDryRun() {
		return dryRun;
	}

	public void setDryRun(boolean dryRun) {
		this.dryRun = dryRun;
	}
	
	void end(boolean closeConnection) throws SQLException {
		if(closeConnection && conn!=null) {
			try {
				getLogger().debug("closing connection: "+conn);
				//conn.rollback();
				conn.close();
			}
			catch(Exception e) {
				getLogger().warn("exception in close(): "+e); 
			}
		}
		getLogger().info("...done");
	}

}
