package tbrugz.sqldump;

import java.io.IOException;
import java.sql.DatabaseMetaData;
import java.sql.SQLException;
import java.util.Properties;

import org.apache.log4j.Logger;

public class DBMSResources {

	static Logger log = Logger.getLogger(DBMSResources.class);
	
	static final String PROP_FROM_DB_ID_AUTODETECT = "sqldump.fromdbid.autodetect";
	
	String dbId;
	Properties papp;
	Properties dbmsSpecificResource = new ParametrizedProperties();
	
	{
		try {
			dbmsSpecificResource.load(JDBCSchemaGrabber.class.getClassLoader().getResourceAsStream(SQLDump.DBMS_SPECIFIC_RESOURCE));
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	public void setup(Properties prop) {
		papp = prop;
	}

	/*private void setup(Properties prop, DatabaseMetaData dbmd) {
		setup(prop);
		updateMetaData(dbmd);
	}*/
	
	void updateMetaData(DatabaseMetaData dbmd) {
		if(Utils.getPropBool(papp, PROP_FROM_DB_ID_AUTODETECT) && dbmd !=null) {
			String dbid = detectDbId(dbmd);
			if(dbid!=null) {
				log.info("database type identifier: "+dbid);
				this.dbId = dbid;
				//FIXME: 
				//papp.setProperty(SQLDump.PROP_FROM_DB_ID, dbid);
				//propOriginal.setProperty(SQLDump.PROP_FROM_DB_ID, dbid);
			}
			else { log.warn("can't detect database type"); }
		}
		else {
			this.dbId = papp.getProperty(SQLDump.PROP_FROM_DB_ID);
		}
	}
	
	String detectDbId(DatabaseMetaData dbmd) {
		try {
			String dbProdName = dbmd.getDatabaseProductName();
			String dbIdsProp = dbmsSpecificResource.getProperty("dbids");
			String[] dbIds = dbIdsProp.split(",");
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

	static String DEFAULT_QUOTE_STRING = "\"";
	
	/* DatabaseMetaData.getIdentifierQuoteString() already does it */
	@Deprecated
	public String getSQLQuoteString() {
		//log.debug("quote ["+dbId+"]: "+dbmsSpecificResource.getProperty("dbid."+dbId+".sqlquotestring"));
		return dbmsSpecificResource.getProperty("dbid."+dbId+".sqlquotestring", DEFAULT_QUOTE_STRING);
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
}
