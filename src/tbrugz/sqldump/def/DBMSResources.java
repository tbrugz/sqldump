package tbrugz.sqldump.def;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.sql.DatabaseMetaData;
import java.sql.SQLException;
import java.util.List;
import java.util.Properties;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import tbrugz.sqldump.dbmd.DBMSFeatures;
import tbrugz.sqldump.dbmodel.Column;
import tbrugz.sqldump.util.IOUtil;
import tbrugz.sqldump.util.ParametrizedProperties;
import tbrugz.sqldump.util.SQLIdentifierDecorator;
import tbrugz.sqldump.util.Utils;

//TODOne: add addUpdateListener() ? so DBMSResources may notify others that need its info
public final class DBMSResources {

	static final Log log = LogFactory.getLog(DBMSResources.class);

	static final String DEFAULT_QUOTE_STRING = "\"";
	
	public static final String DEFAULT_DBID = "<default>"; 
	
	static final String PROP_FROM_DB_ID_AUTODETECT = "sqldump.fromdbid.autodetect";
	
	static final String PREFIX_DBMS = "sqldump.dbms";
	static final String SUFFIX_SPECIFICGRABCLASS = ".specificgrabclass";
	static final String PROP_DBMS_SPECIFICGRABCLASS = PREFIX_DBMS + SUFFIX_SPECIFICGRABCLASS;

	static final DBMSResources instance = new DBMSResources();
	
	final Properties papp = new Properties(); //TODO: maybe DBMSResources should not depend on processProperties...
	final Properties dbmsSpecificResource = new ParametrizedProperties();

	@Deprecated String dbId;
	//@Deprecated String identifierQuoteString = DEFAULT_QUOTE_STRING;
	@Deprecated DBMSFeatures features;
	
	//final Set<DBMSUpdateListener> updateListeners = new HashSet<DBMSUpdateListener>();
	
	final List<String> dbIds; // = new ArrayList<String>();
	
	protected DBMSResources() {
		try {
			dbmsSpecificResource.load(IOUtil.getResourceAsStream(Defs.DBMS_SPECIFIC_RESOURCE));
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
	
	/*@Deprecated
	protected void updateMetaData(DatabaseMetaData dbmd) {
		updateMetaData(dbmd, false);
	}*/
	
	/*@Deprecated
	protected void updateMetaData(DatabaseMetaData dbmd, boolean quiet) {
		String dbIdTmp = papp.getProperty(Defs.PROP_FROM_DB_ID);
		if( (Utils.getPropBool(papp, PROP_FROM_DB_ID_AUTODETECT, true) || dbIdTmp==null) && dbmd != null) {
			String dbid = detectDbId(dbmd, quiet);
			if(dbid!=null) {
				if(!quiet) { log.info("database type identifier: "+dbid); }
				updateDbId(dbid);
			}
			else {
				log.warn("can't detect database type");
				//updateIdentifierQuoteString();
				updateSpecificFeaturesClass(DBMSResources.instance().dbid());
				fireUpdateToListeners();
			}
		}
		else {
			updateDbId(dbIdTmp);
			if(!quiet) { log.info("database type identifier ('"+Defs.PROP_FROM_DB_ID+"'): "+this.dbId); }
		}

		//updateIdentifierQuoteString();
	}*/
	
	/*@Deprecated
	protected void updateDbId(String newid) {
		if( (newid!=null) && newid.equals(dbId) ) {
			log.debug("same dbid, no update");
			return;
		}
		
		log.debug("updating dbid: '"+newid+"' [old="+dbId+"]");
		if(dbIds.contains(newid) || newid==null) {
			dbId = newid;
			//updateIdentifierQuoteString();
			updateSpecificFeaturesClass(dbId);
			fireUpdateToListeners();
		}
		else {
			log.warn("unknown dbid: '"+newid+"' ; keeping '"+dbId+"' as dbid");
		}
	}*/
	
	/*@Deprecated
	void updateIdentifierQuoteString() {
		identifierQuoteString = dbmsSpecificResource.getProperty("dbid."+dbId+".sqlquotestring", 
				//(identifierQuoteString!=null ? identifierQuoteString : DEFAULT_QUOTE_STRING)
				DEFAULT_QUOTE_STRING
				);
		SQLIdentifierDecorator.dumpIdentifierQuoteString = identifierQuoteString;
	}*/
	
	public String detectDbId(DatabaseMetaData dbmd) {
		return detectDbId(dbmd, true);
	}
	
	//TODO: also detect by getUrl()...
	public String detectDbId(DatabaseMetaData dbmd, boolean quiet) {
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

	//@Deprecated
	/* DatabaseMetaData.getIdentifierQuoteString() already does it */
	//public String getIdentifierQuoteString() {
	//	return identifierQuoteString;
		//log.debug("quote ["+dbId+"]: "+dbmsSpecificResource.getProperty("dbid."+dbId+".sqlquotestring"));
		//return dbmsSpecificResource.getProperty("dbid."+dbId+".sqlquotestring", DEFAULT_QUOTE_STRING);
	//}
	
	/*
	@Deprecated
	protected String dbid() {
		return dbId;
	}
	
	@Deprecated
	protected DBMSFeatures databaseSpecificFeaturesClass() {
		if(features!=null) { return features; }
		
		updateSpecificFeaturesClass(DBMSResources.instance().dbid());
		
		return features;
	}
	*/
	
	/*@Deprecated
	synchronized void updateSpecificFeaturesClass() {
		features = getSpecificFeatures(DBMSResources.instance().dbid());
	}*/
	
	public synchronized void updateSpecificFeaturesClass(String dbid) {
		features = getSpecificFeatures(dbid);
		SQLIdentifierDecorator.dumpIdentifierQuoteString = features.getIdentifierQuoteString();
		/*
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
			features = new DefaultDBMSFeatures();
			log.debug("no specific DBMS features defined. using "+features.getClass().getSimpleName());
		}
		initDBMSFeatures(features, papp);
		*/
	}

	public DBMSFeatures getSpecificFeatures(String dbid) {
		String dbSpecificFeaturesClass = null;
		DBMSFeatures feats = null;
		
		if(dbid==null) {
			dbid = DEFAULT_DBID;
		}
		
		String specificKey = PREFIX_DBMS+"@"+dbid+SUFFIX_SPECIFICGRABCLASS;
		dbSpecificFeaturesClass = papp.getProperty(specificKey);
		if(dbSpecificFeaturesClass!=null) {
			log.info("using specific grab class for ["+dbid+"]: "+dbSpecificFeaturesClass);
		}
		else {
			dbSpecificFeaturesClass = papp.getProperty(PROP_DBMS_SPECIFICGRABCLASS);
			if(dbSpecificFeaturesClass!=null) {
				log.info("using *global* specific grab class: "+dbSpecificFeaturesClass);
			}
			else {
				dbSpecificFeaturesClass = dbmsSpecificResource.getProperty("dbms."+dbid+".specificgrabclass");
			}
		}
		
		if(dbSpecificFeaturesClass!=null) {
			//XXX: call Utils.getClassByName()
			try {
				Class<?> c = Utils.loadClass(dbSpecificFeaturesClass);
				feats = (DBMSFeatures) c.getConstructor().newInstance();
				
				feats.setId(dbid);
				log.debug("specific DBMS features class: "+c.getName());
			} catch (ClassNotFoundException e) {
				throw new RuntimeException(e);
			} catch (InstantiationException e) {
				throw new RuntimeException(e);
			} catch (IllegalAccessException e) {
				throw new RuntimeException(e);
			} catch (NoSuchMethodException e) {
				throw new RuntimeException(e);
			} catch (InvocationTargetException e) {
				throw new RuntimeException(e);
			}
		}
		else {
			//if(features==null) ?
			log.warn("unknown dbid: "+dbid);
			throw new RuntimeException("unknown dbid: "+dbid);
			//feats = new DefaultDBMSFeatures(dbid);
			//log.warn("no specific DBMS features defined for '"+dbid+"'. using "+feats.getClass().getSimpleName());
		}
		initDBMSFeatures(feats, papp);
		return feats;
	}
	
	public DBMSFeatures getSpecificFeatures(DatabaseMetaData dbmd) {
		return getSpecificFeatures(dbmd, true);
	}
	
	public DBMSFeatures getSpecificFeatures(DatabaseMetaData dbmd, boolean quiet) {
		String dbid = detectDbId(dbmd, quiet);
		return getSpecificFeatures(dbid);
	}
	
	protected void initDBMSFeatures(DBMSFeatures feats, Properties prop) {
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
	
	/*
	public void addUpdateListener(DBMSUpdateListener listener) {
		updateListeners.add(listener);
	}
	
	public boolean removeUpdateListener(DBMSUpdateListener listener) {
		return updateListeners.remove(listener);
	}
	*/
	
	/*protected void fireUpdateToListeners() {
		for(DBMSUpdateListener listener: updateListeners) {
			listener.dbmsUpdated();
		}
		
		//instead of firing update to listeners...
		Column.ColTypeUtil.setDbId(DBMSResources.instance().dbid());
		Map<Class<?>, Class<?>> mapper = DBMSResources.instance().databaseSpecificFeaturesClass().getColumnTypeMapper();
		SQLUtils.setupColumnTypeMapper(mapper);
		//ColumnDiff.updateFeatures(DBMSResources.instance().getSpecificFeatures(s));
	}*/
	
}
