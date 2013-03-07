package tbrugz.sqldump.def;

import java.io.IOException;
import java.sql.DatabaseMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import tbrugz.sqldump.util.ParametrizedProperties;
import tbrugz.sqldump.util.SQLIdentifierDecorator;
import tbrugz.sqldump.util.Utils;

public class DBMSResources {

	static Log log = LogFactory.getLog(DBMSResources.class);

	static final String DEFAULT_QUOTE_STRING = "\"";
	
	static final String PROP_FROM_DB_ID_AUTODETECT = "sqldump.fromdbid.autodetect";
	
	String dbId;
	Properties papp = new Properties();
	Properties dbmsSpecificResource = new ParametrizedProperties();
	String identifierQuoteString = DEFAULT_QUOTE_STRING;
	
	List<String> dbIds = new ArrayList<String>();
	
	{
		try {
			dbmsSpecificResource.load(DBMSResources.class.getClassLoader().getResourceAsStream(Defs.DBMS_SPECIFIC_RESOURCE));
			dbIds = Utils.getStringListFromProp(dbmsSpecificResource, "dbids", ",");
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	public void setup(Properties prop) {
		if(prop==null) {
			log.warn("trying to set null properties...");
			return;
		}
		papp = prop;
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
				this.dbId = dbid;
			}
			else { log.warn("can't detect database type"); }
		}
		else {
			this.dbId = dbIdTmp;
			log.info("database type identifier ('"+Defs.PROP_FROM_DB_ID+"'): "+this.dbId);
		}

		identifierQuoteString = DEFAULT_QUOTE_STRING;
		
		if(dbmd!=null) {
			try {
				identifierQuoteString = dbmd.getIdentifierQuoteString();
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}
		//value from dbms-specific.properties::dbid.mysql.sqlquotestring takes precedence  
		updateIdentifierQuoteString();
		
		SQLIdentifierDecorator.dumpIdentifierQuoteString = identifierQuoteString;
	}
	
	public void updateDbId(String newid) {
		DBMSResources res = this;
		if( (newid!=null && newid.equals(res.dbId)) || (newid==res.dbId) ) { return; }
		
		log.info("updating dbid: '"+newid+"' [old="+res.dbId+"]");
		if(res.dbIds.contains(newid) || newid==null) {
			res.dbId = newid;
			res.updateIdentifierQuoteString();
		}
		else {
			log.warn("unknown dbid: '"+newid+"' ; keeping '"+res.dbId+"' as dbid");
		}
	}
	
	void updateIdentifierQuoteString() {
		identifierQuoteString = dbmsSpecificResource.getProperty("dbid."+dbId+".sqlquotestring", identifierQuoteString);
	}
	
	String detectDbId(DatabaseMetaData dbmd) {
		try {
			String dbProdName = dbmd.getDatabaseProductName();
			//String dbIdsProp = dbmsSpecificResource.getProperty("dbids");
			//String[] dbIds = dbIdsProp.split(",");
			for(String dbid: dbIds) {
				dbid = dbid.trim();
				String regex = dbmsSpecificResource.getProperty("dbid."+dbid+".detectregex");
				if(regex==null) { continue; }
				if(!dbProdName.matches(regex)) { continue; }
				
				Long majorVersionEQorLess = Utils.getPropLong(dbmsSpecificResource, "dbid."+dbid+".detectversion.major.equalorless");

				if(majorVersionEQorLess==null) { return dbid; }
				if(dbmd.getDatabaseMajorVersion() < majorVersionEQorLess) { return dbid; }
				if(dbmd.getDatabaseMajorVersion() > majorVersionEQorLess) { continue; }

				//equal major version...
				Long minorVersionEQorLess = Utils.getPropLong(dbmsSpecificResource, "dbid."+dbid+".detectversion.minor.equalorless");
				if(minorVersionEQorLess==null) { return dbid; }
				//if(dbmd.getDatabaseMinorVersion() <= minorVersionEQorLess) { return dbid; }
				if(dbmd.getDatabaseMinorVersion() > minorVersionEQorLess) { continue; }
				
				return dbid;
			}
		} catch (SQLException e) {
			log.warn("Error detecting database type: "+e);
			log.debug("Error detecting database type",e);
		}
		return null;
	}
	
	/*public void setDbId(String dbId) {
		this.dbId = dbId;
	}*/

	/*DBMSFeatures grabDbSpecificFeaturesClass() {
		String dbSpecificFeaturesClass = dbmsSpecificResource.getProperty("dbms."+dbId+".specificgrabclass");
		if(dbSpecificFeaturesClass!=null) {
			//XXX: call Utils.getClassByName()
			try {
				Class<?> c = Class.forName(dbSpecificFeaturesClass);
				DBMSFeatures of = (DBMSFeatures) c.newInstance();
				of.procProperties(papp);
				return of;
			} catch (ClassNotFoundException e) {
				e.printStackTrace();
			} catch (InstantiationException e) {
				e.printStackTrace();
			} catch (IllegalAccessException e) {
				e.printStackTrace();
			}
		}
		return new DefaultDBMSFeatures();
	}*/
	
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
	
	public DBMSFeatures databaseSpecificFeaturesClass() {
		String dbSpecificFeaturesClass = dbmsSpecificResource.getProperty("dbms."+DBMSResources.instance().dbid()+".specificgrabclass");
		if(dbSpecificFeaturesClass!=null) {
			//XXX: call Utils.getClassByName()
			try {
				Class<?> c = Class.forName(dbSpecificFeaturesClass);
				DBMSFeatures of = (DBMSFeatures) c.newInstance();
				of.procProperties(papp);
				return of;
			} catch (ClassNotFoundException e) {
				e.printStackTrace();
			} catch (InstantiationException e) {
				e.printStackTrace();
			} catch (IllegalAccessException e) {
				e.printStackTrace();
			}
		}
		return new DefaultDBMSFeatures();
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
