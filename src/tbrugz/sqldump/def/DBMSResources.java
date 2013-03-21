package tbrugz.sqldump.def;

import java.io.IOException;
import java.sql.DatabaseMetaData;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import tbrugz.sqldump.SQLUtils;
import tbrugz.sqldump.util.ParametrizedProperties;
import tbrugz.sqldump.util.SQLIdentifierDecorator;
import tbrugz.sqldump.util.Utils;

public class DBMSResources {

	static final Log log = LogFactory.getLog(DBMSResources.class);

	static final String DEFAULT_QUOTE_STRING = "\"";
	
	static final String PROP_FROM_DB_ID_AUTODETECT = "sqldump.fromdbid.autodetect";
	static final String PROP_DBMS_SPECIFICGRABCLASS = "sqldump.dbms.specificgrabclass";
	
	final Properties papp = new Properties();
	final Properties dbmsSpecificResource = new ParametrizedProperties();

	String dbId;
	String identifierQuoteString = DEFAULT_QUOTE_STRING;
	
	final List<String> dbIds; // = new ArrayList<String>();
	
	protected DBMSResources() {
		try {
			dbmsSpecificResource.load(DBMSResources.class.getClassLoader().getResourceAsStream(Defs.DBMS_SPECIFIC_RESOURCE));
		} catch (IOException e) {
			log.warn("error loading resource: "+Defs.DBMS_SPECIFIC_RESOURCE);
		}
		dbIds = Utils.getStringListFromProp(dbmsSpecificResource, "dbids", ",");
	}
	
	public void setup(Properties prop) {
		if(prop==null) {
			log.warn("trying to set null properties...");
			return;
		}
		papp.clear();
		papp.putAll(prop);
	}

	/*private void setup(Properties prop, DatabaseMetaData dbmd) {
		setup(prop);
		updateMetaData(dbmd);
	}*/
	
	public void updateMetaData(DatabaseMetaData dbmd) {
		String dbIdTmp = papp.getProperty(Defs.PROP_FROM_DB_ID);
		if( (Utils.getPropBool(papp, PROP_FROM_DB_ID_AUTODETECT) || dbIdTmp==null) && dbmd != null) {
			String dbid = detectDbId(dbmd);
			if(dbid!=null) {
				log.info("database type identifier: "+dbid);
				updateDbId(dbid);
			}
			else {
				log.warn("can't detect database type");
				updateIdentifierQuoteString();
			}
		}
		else {
			updateDbId(dbIdTmp);
			log.info("database type identifier ('"+Defs.PROP_FROM_DB_ID+"'): "+this.dbId);
		}

		SQLIdentifierDecorator.dumpIdentifierQuoteString = identifierQuoteString;
	}
	
	public void updateDbId(String newid) {
		if( (newid!=null) && newid.equals(dbId) ) {
			log.debug("same dbid, no update");
			return;
		}
		
		log.info("updating dbid: '"+newid+"' [old="+dbId+"]");
		if(dbIds.contains(newid) || newid==null) {
			dbId = newid;
			updateIdentifierQuoteString();
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
	String detectDbId(DatabaseMetaData dbmd) {
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
			log.info("detectDbId: product = "+dbProdName+"; majorVersion = "+dbMajorVersion+"; minorVersion = "+dbMinorVersion);
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
				//if(dbmd.getDatabaseMinorVersion() <= minorVersionEQorLess) { return dbid; }
				if(dbMinorVersion > minorVersionEQorLess) { continue; }
				
				return dbid;
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
	
	static DBMSResources instance;
	
	public static DBMSResources instance() {
		if(instance==null) {
			instance = new DBMSResources();
		}
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
	
	//TODO: cache DBMSFeatures?
	public DBMSFeatures databaseSpecificFeaturesClass() {
		String dbSpecificFeaturesClass = null;
		
		dbSpecificFeaturesClass = papp.getProperty(PROP_DBMS_SPECIFICGRABCLASS,
				dbmsSpecificResource.getProperty("dbms."+DBMSResources.instance().dbid()+".specificgrabclass"));
		
		
		if(dbSpecificFeaturesClass!=null) {
			//XXX: call Utils.getClassByName()
			try {
				Class<?> c = Class.forName(dbSpecificFeaturesClass);
				DBMSFeatures of = (DBMSFeatures) c.newInstance();
				initDBMSFeatures(of, papp);
				log.debug("specific DBMS features class: "+c.getName());
				return of;
			} catch (ClassNotFoundException e) {
				e.printStackTrace();
			} catch (InstantiationException e) {
				e.printStackTrace();
			} catch (IllegalAccessException e) {
				e.printStackTrace();
			}
		}
		log.info("no specific DBMS features defined. using "+DefaultDBMSFeatures.class.getSimpleName());
		return new DefaultDBMSFeatures();
	}
	
	void initDBMSFeatures(DBMSFeatures feats, Properties prop) {
		feats.procProperties(prop);
		Map<Class<?>, Class<?>> mapper = feats.getColumnTypeMapper();
		if(mapper!=null) {
			SQLUtils.setupColumnTypeMapper(mapper);
			log.debug("column-mapper: "+mapper);
		}
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
	
}
