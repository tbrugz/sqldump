package tbrugz.sqldump.def;

import java.io.IOException;
import java.sql.DatabaseMetaData;
import java.sql.SQLException;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import tbrugz.sqldump.dbmd.DBMSFeatures;
import tbrugz.sqldump.dbmd.DefaultDBMSFeatures;
import tbrugz.sqldump.dbmodel.Column;
import tbrugz.sqldump.util.ParametrizedProperties;
import tbrugz.sqldump.util.SQLIdentifierDecorator;
import tbrugz.sqldump.util.SQLUtils;
import tbrugz.sqldump.util.Utils;

//TODOne: add addUpdateListener() ? so DBMSResources may notify others that need its info
public final class DBMSResources {

	static final Log log = LogFactory.getLog(DBMSResources.class);

	static final String DEFAULT_QUOTE_STRING = "\"";
	
	static final String PROP_FROM_DB_ID_AUTODETECT = "sqldump.fromdbid.autodetect";
	static final String PROP_DBMS_SPECIFICGRABCLASS = "sqldump.dbms.specificgrabclass";

	static final DBMSResources instance = new DBMSResources();
	
	final Properties papp = new Properties(); //TODO: maybe DBMSResources should not depend on processProperties...
	final Properties dbmsSpecificResource = new ParametrizedProperties();

	String dbId;
	String identifierQuoteString = DEFAULT_QUOTE_STRING;
	DBMSFeatures features;
	
	final Set<DBMSUpdateListener> updateListeners = new HashSet<DBMSUpdateListener>();
	
	final List<String> dbIds; // = new ArrayList<String>();
	
	protected DBMSResources() {
		try {
			dbmsSpecificResource.load(DBMSResources.class.getClassLoader().getResourceAsStream(Defs.DBMS_SPECIFIC_RESOURCE));
		} catch (IOException e) {
			log.warn("error loading resource: "+Defs.DBMS_SPECIFIC_RESOURCE);
		}
		dbIds = Utils.getStringListFromProp(dbmsSpecificResource, "dbids", ",");

		Column.ColTypeUtil.init(dbmsSpecificResource);
	}
	
	public void setup(Properties prop) {
		papp.clear();
		if(prop==null) {
			log.warn("setting null properties...");
		}
		else {
			papp.putAll(prop);
		}
	}

	/*private void setup(Properties prop, DatabaseMetaData dbmd) {
		setup(prop);
		updateMetaData(dbmd);
	}*/
	
	public void updateMetaData(DatabaseMetaData dbmd) {
		updateMetaData(dbmd, false);
	}
	
	public void updateMetaData(DatabaseMetaData dbmd, boolean quiet) {
		String dbIdTmp = papp.getProperty(Defs.PROP_FROM_DB_ID);
		if( (Utils.getPropBool(papp, PROP_FROM_DB_ID_AUTODETECT, true) || dbIdTmp==null) && dbmd != null) {
			String dbid = detectDbId(dbmd, quiet);
			if(dbid!=null) {
				if(!quiet) { log.info("database type identifier: "+dbid); }
				updateDbId(dbid);
			}
			else {
				log.warn("can't detect database type");
				updateIdentifierQuoteString();
				updateSpecificFeaturesClass();
				fireUpdateToListeners();
			}
		}
		else {
			updateDbId(dbIdTmp);
			if(!quiet) { log.info("database type identifier ('"+Defs.PROP_FROM_DB_ID+"'): "+this.dbId); }
		}

		SQLIdentifierDecorator.dumpIdentifierQuoteString = identifierQuoteString;
	}
	
	public void updateDbId(String newid) {
		if( (newid!=null) && newid.equals(dbId) ) {
			log.debug("same dbid, no update");
			return;
		}
		
		log.debug("updating dbid: '"+newid+"' [old="+dbId+"]");
		if(dbIds.contains(newid) || newid==null) {
			dbId = newid;
			updateIdentifierQuoteString();
			updateSpecificFeaturesClass();
			fireUpdateToListeners();
		}
		else {
			log.warn("unknown dbid: '"+newid+"' ; keeping '"+dbId+"' as dbid");
		}
	}
	
	void updateIdentifierQuoteString() {
		identifierQuoteString = dbmsSpecificResource.getProperty("dbid."+dbId+".sqlquotestring", 
				(identifierQuoteString!=null ? identifierQuoteString : DEFAULT_QUOTE_STRING)
				);
	}
	
	//TODO: also detect by getUrl()...
	String detectDbId(DatabaseMetaData dbmd, boolean quiet) {
		try {
			String dbProdName = dbmd.getDatabaseProductName();
			int dbMajorVersion = -1;
			int dbMinorVersion = -1;
			try {
				dbMajorVersion = dbmd.getDatabaseMajorVersion();
				dbMinorVersion = dbmd.getDatabaseMinorVersion();
			}
			catch(Exception e) {
				log.warn("error getting metadata: "+e);
			}
			catch(LinkageError e) {
				log.warn("error getting metadata: "+e);
			}
			if(!quiet) { log.info("detectDbId: product = "+dbProdName+"; majorVersion = "+dbMajorVersion+"; minorVersion = "+dbMinorVersion); }
			//String dbIdsProp = dbmsSpecificResource.getProperty("dbids");
			//String[] dbIds = dbIdsProp.split(",");
			for(String dbid: dbIds) {
				dbid = dbid.trim();
				String regex = dbmsSpecificResource.getProperty("dbid."+dbid+".detectregex");
				if(regex==null) { continue; }
				if(!dbProdName.matches(regex)) { continue; }
				
				Long majorVersionEQorLess = Utils.getPropLong(dbmsSpecificResource, "dbid."+dbid+".detectversion.major.equalorless");

				if(majorVersionEQorLess==null) { return dbid; }
				if(dbMajorVersion < majorVersionEQorLess) { return dbid; }
				if(dbMajorVersion > majorVersionEQorLess) { continue; }

				//equal major version...
				Long minorVersionEQorLess = Utils.getPropLong(dbmsSpecificResource, "dbid."+dbid+".detectversion.minor.equalorless");
				if(minorVersionEQorLess==null) { return dbid; }
				if(dbMinorVersion <= minorVersionEQorLess) { return dbid; }
			}
			
			/*String dbUrl = dbmd.getURL();
			for(String dbid: dbIds) {
				dbid = dbid.trim();
				String regex = dbmsSpecificResource.getProperty("dbid."+dbid+".detectregex.url");
				log.info("db: "+dbid+" url: "+dbUrl+" ; urlregex: "+regex);
				if(regex==null) { continue; }
				if(!dbUrl.matches(regex)) { continue; }
				
				return dbid;
			}*/
			log.warn("unknown database type: product name = '"+dbProdName+"'");
		} catch (SQLException e) {
			log.warn("Error detecting database type: "+e);
			log.debug("Error detecting database type",e);
		}
		return null;
	}
	
	public static DBMSResources instance() {
		/*if(instance==null) {
			instance = new DBMSResources();
		}*/
		return instance;
	}

	/* DatabaseMetaData.getIdentifierQuoteString() already does it */
	public String getIdentifierQuoteString() {
		return identifierQuoteString;
		//log.debug("quote ["+dbId+"]: "+dbmsSpecificResource.getProperty("dbid."+dbId+".sqlquotestring"));
		//return dbmsSpecificResource.getProperty("dbid."+dbId+".sqlquotestring", DEFAULT_QUOTE_STRING);
	}
	
	public String dbid() {
		return dbId;
	}
	
	//TODOne: cache DBMSFeatures?
	public DBMSFeatures databaseSpecificFeaturesClass() {
		if(features!=null) { return features; }
		
		updateSpecificFeaturesClass();
		
		return features;
	}
	
	synchronized void updateSpecificFeaturesClass() {
		String dbSpecificFeaturesClass = null;
		
		dbSpecificFeaturesClass = papp.getProperty(PROP_DBMS_SPECIFICGRABCLASS);
		if(dbSpecificFeaturesClass!=null) {
			log.info("using specific grab class: "+dbSpecificFeaturesClass);
		}
		else {
			dbSpecificFeaturesClass = dbmsSpecificResource.getProperty("dbms."+DBMSResources.instance().dbid()+".specificgrabclass");
		}
		
		if(dbSpecificFeaturesClass!=null) {
			//XXX: call Utils.getClassByName()
			try {
				Class<?> c = Class.forName(dbSpecificFeaturesClass);
				features = (DBMSFeatures) c.newInstance();
				log.debug("specific DBMS features class: "+c.getName());
			} catch (ClassNotFoundException e) {
				throw new RuntimeException(e);
			} catch (InstantiationException e) {
				throw new RuntimeException(e);
			} catch (IllegalAccessException e) {
				throw new RuntimeException(e);
			}
		}
		else {
			//if(features==null) ?
			log.debug("no specific DBMS features defined. using "+DefaultDBMSFeatures.class.getSimpleName());
			features = new DefaultDBMSFeatures();
		}
		initDBMSFeatures(features, papp);
	}
	
	void initDBMSFeatures(DBMSFeatures feats, Properties prop) {
		feats.procProperties(prop);
		//TODOne: all classes that use DBMSSpecific features should be called here? what about a listener?
	}
	
	public String getPrivileges(String dbId) {
		return dbmsSpecificResource.getProperty("privileges."+dbId);
	}
	
	public String toANSIType(String sqlDialect, String columnType) {
		return dbmsSpecificResource.getProperty("from."+sqlDialect+"."+columnType);
	}

	public String toSQLDialectType(String sqlDialect, String ansiColumnType) {
		return dbmsSpecificResource.getProperty("to."+sqlDialect+"."+ansiColumnType);
	}

	public List<String> getDbIds() {
		return dbIds;
	}
	
	//XXX: getProperties(): not a good idea?
	public Properties getProperties() {
		return dbmsSpecificResource;
	}
	
	/*void reset() {
		dbId = null;
		identifierQuoteString = DEFAULT_QUOTE_STRING;
		features = new DefaultDBMSFeatures();
		initDBMSFeatures(features, null);
	}*/
	
	public void addUpdateListener(DBMSUpdateListener listener) {
		updateListeners.add(listener);
	}
	
	public boolean removeUpdateListener(DBMSUpdateListener listener) {
		return updateListeners.remove(listener);
	}
	
	void fireUpdateToListeners() {
		for(DBMSUpdateListener listener: updateListeners) {
			listener.dbmsUpdated();
		}
		
		//instead of firing update to listeners...
		Column.ColTypeUtil.setDbId(DBMSResources.instance().dbid());
		Map<Class<?>, Class<?>> mapper = DBMSResources.instance().databaseSpecificFeaturesClass().getColumnTypeMapper();
		SQLUtils.setupColumnTypeMapper(mapper);
	}
	
}
