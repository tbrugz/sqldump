package tbrugz.sqlmigrate.util;

import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import tbrugz.sqldiff.SchemaDiffer;
import tbrugz.sqldiff.model.ChangeType;
import tbrugz.sqldiff.model.ColumnDiff;
import tbrugz.sqldiff.model.Diff;
import tbrugz.sqldiff.model.SchemaDiff;
import tbrugz.sqldump.JAXBSchemaXMLSerializer;
import tbrugz.sqldump.JDBCSchemaGrabber;
import tbrugz.sqldump.dbmd.AbstractDBMSFeatures;
import tbrugz.sqldump.dbmd.DBMSFeatures;
import tbrugz.sqldump.dbmodel.DBIdentifiable;
import tbrugz.sqldump.dbmodel.SchemaModel;
import tbrugz.sqldump.dbmodel.Table;
import tbrugz.sqldump.def.DBMSResources;
import tbrugz.sqldump.def.Defs;
import tbrugz.sqldump.def.Processor;
import tbrugz.sqldump.processors.SQLDialectTransformer;
import tbrugz.sqldump.util.ConnectionUtil;

public class SchemaUtils {

	static final Log log = LogFactory.getLog(SchemaUtils.class);

	static final String FALSE = "false";
	static final String TRUE = "true";

	static final String MODEL_PATH = "/tbrugz/sqlmigrate/sqlmigrate-schema.xml";
	
	static Properties getJdbcGrabProperties(String schemaNames, String tableNames) {
		Properties grabProps = new Properties();
		grabProps.put(JDBCSchemaGrabber.PROP_SCHEMAGRAB_PROCEDURESANDFUNCTIONS, FALSE);
		grabProps.put(JDBCSchemaGrabber.PROP_SCHEMAGRAB_DBSPECIFIC, TRUE);
		if(schemaNames!=null) {
			grabProps.put(Defs.PROP_SCHEMAGRAB_SCHEMANAMES, schemaNames);
		}
		if(tableNames!=null) {
			grabProps.put(JDBCSchemaGrabber.PROP_SCHEMAGRAB_TABLEFILTER, tableNames);
		}
		//List<String> typesList = Arrays.asList(new String[]{"SCHEMA_META", "TABLE", "FK", "CONSTRAINT"});
		List<String> typesList = Arrays.asList(new String[]{"SCHEMA_META", "TABLE", "CONSTRAINT"});
		setPropForTypes(grabProps, typesList);
		return grabProps;
	}
	
	static void setPropForTypes(Properties prop, List<String> typesToGrab) {
		Set<String> trueProps = new HashSet<String>();
		
		String[] types = { "TABLE", "FK", "VIEW", "INDEX", "TRIGGER",
				"SEQUENCE", "SYNONYM", "GRANT", "MATERIALIZED_VIEW", "CONSTRAINT",
				"FUNCTION", "PACKAGE", "PACKAGE_BODY", "PROCEDURE", "TYPE" };
				
		String[] props = { JDBCSchemaGrabber.PROP_SCHEMAGRAB_TABLES, JDBCSchemaGrabber.PROP_SCHEMAGRAB_FKS /* exportedfks?*/, AbstractDBMSFeatures.PROP_GRAB_VIEWS, AbstractDBMSFeatures.PROP_GRAB_INDEXES, AbstractDBMSFeatures.PROP_GRAB_TRIGGERS,
				AbstractDBMSFeatures.PROP_GRAB_SEQUENCES, AbstractDBMSFeatures.PROP_GRAB_SYNONYMS, JDBCSchemaGrabber.PROP_SCHEMAGRAB_GRANTS, AbstractDBMSFeatures.PROP_GRAB_MATERIALIZED_VIEWS, AbstractDBMSFeatures.PROP_GRAB_CONSTRAINTS_XTRA,
				AbstractDBMSFeatures.PROP_GRAB_EXECUTABLES, AbstractDBMSFeatures.PROP_GRAB_EXECUTABLES, AbstractDBMSFeatures.PROP_GRAB_EXECUTABLES, AbstractDBMSFeatures.PROP_GRAB_EXECUTABLES, AbstractDBMSFeatures.PROP_GRAB_EXECUTABLES };
		
		for(int i=0;i<types.length; i++) {
			String t = types[i];
			if(typesToGrab.contains(t)) {
				trueProps.add(props[i]);
			}
		}
		
		for(int i=0;i<props.length; i++) {
			String t = props[i];
			if(t==null) { continue; }
			if(trueProps.contains(t)) {
				prop.setProperty(t, TRUE);
			}
			else {
				prop.setProperty(t, FALSE);
			}
		}
	}
	
	public static SchemaModel getModelFromXmlResource(String resourcePath) {
		JAXBSchemaXMLSerializer jss = new JAXBSchemaXMLSerializer();
		Properties p = new Properties();
		p.setProperty(JAXBSchemaXMLSerializer.XMLSERIALIZATION_JAXB_DEFAULT_PREFIX +
				JAXBSchemaXMLSerializer.PROP_XMLSERIALIZATION_JAXB_INRESOURCE,
				resourcePath);
		jss.setProperties(p);
		return jss.grabSchema();
	}
	
	public static SchemaModel getDbModel(Connection conn, String schemaNames, String tableName) {
		JDBCSchemaGrabber jsg = new JDBCSchemaGrabber();
		jsg.setConnection(conn);
		jsg.setProperties(getJdbcGrabProperties(schemaNames, tableName));
		SchemaModel dbModel = jsg.grabSchema();
		return dbModel;
	}
	
	public static void transformDialectBackAndForth(SchemaModel schema, String dialect) {
		log.debug("transformDialectBackAndForth: SQLDialectTransformer: todbid dialect="+dialect);
		Processor transf = new SQLDialectTransformer();
		transf.setSchemaModel(schema);
		
		{
			Properties transformProps = new Properties();
			//"sqldump.schematransform.toansi"
			transformProps.setProperty(SQLDialectTransformer.PROP_TRANSFORM_TO_ANSI, TRUE);
			transf.setProperties(transformProps);
			transf.process();
		}

		{
			Properties transformProps = new Properties();
			// "sqldump.schematransform.todbid"
			transformProps.setProperty(SQLDialectTransformer.PROP_TRANSFORM_TO_DBID, dialect);
			transf.setProperties(transformProps);
			transf.process();
		}
	}
	
	public static void removeDiffsOfTypes(Set<? extends Diff> diffSet, List<ChangeType> types) {
		int removed = 0;
		Iterator<? extends Diff> it = diffSet.iterator();
		while(it.hasNext()) {
			Diff diff = it.next();
			if(types.contains(diff.getChangeType())) {
				log.debug("will not generate "+diff.getChangeType()+" diff for "+diff);
				it.remove();
				removed++;
			}
		}
		log.debug("diff: removed "+removed+" diffs");
	}
	
	public static void applyDiffs(List<Diff> diffs, Connection conn) throws IOException, SQLException {
		String sql = null;
		try {
			int executeCount = 0;
			//StringBuilder sb = new StringBuilder();
			for(Diff d: diffs) {
				List<String> sqls = d.getDiffList();
				for(String s: sqls) {
					if(s==null || s.equals("")) { continue; }
					sql = s;
					
					Statement st = conn.createStatement();
					log.info("will execute: "+sql);
					//boolean retIsRs = 
					st.execute(sql);
					int count = st.getUpdateCount();
					log.debug("diff executed: "+sql+" [updateCount = "+count+"]");
					executeCount++;
				}
			}
			//DBUtil.doCommit(conn);
			ConnectionUtil.doCommitIfNotAutocommit(conn);
			if(executeCount>0) {
				log.info(executeCount+" diffs applyed");
			}
			else {
				log.debug("no diffs applyed");
			}
		}
		finally {
			//ConnectionUtil.closeConnection(conn);
		}
	}
	
	public static SchemaDiff diffModel(Connection conn, String schemaName, String tableName) {
		// grab models
		SchemaModel model = getModelFromXmlResource(MODEL_PATH);
		if(model.getTables().size()!=1) {
			throw new IllegalStateException("model.getTables().size() != 1 ["+model.getTables().size()+"]");
		}
		Table migrationTable = model.getTables().iterator().next();
		//if(schemaName!=null) {
		migrationTable.setSchemaName(schemaName);
		//}
		migrationTable.setName(tableName);
		SchemaModel dbModel = getDbModel(conn, schemaName, tableName);
		String dialect = dbModel.getSqlDialect();
		transformDialectBackAndForth(dbModel, dialect);
		Set<String> dbSchemaNames = getSchemaNames(dbModel.getTables());
		if(dbSchemaNames.size()==0) {
			log.info("dbModel's tables have no schema name");
		}
		else {
			String dbSchema = dbSchemaNames.iterator().next();
			log.info("dbModel's tables schema name: "+dbSchema); //+" [dbSchemaNames.size()=="+dbSchemaNames.size()+"]");
			/*if(dbSchemaNames.size()==1) {
				log.info("dbModel's tables schema name: "+dbSchema+" [dbSchemaNames.size()=="+dbSchemaNames.size()+"]");
			}
			else {
				log.info("dbModel's tables schema name: "+dbSchema+" [dbSchemaNames.size()=="+dbSchemaNames.size()+"]");
			}*/
			if(dbSchemaNames.size()>1) {
				log.warn("dbModel's tables schemas: "+dbSchemaNames+" [dbSchemaNames.size() > 1 ; #"+dbSchemaNames.size()+"]");
			}
			migrationTable.setSchemaName(dbSchema);
		}
		
		// logging
		log.info("migrationModel.tables: "+model.getTables());
		log.info("dbModel.tables: "+dbModel.getTables());
		
		// diff
		SchemaDiffer differ = new SchemaDiffer();
		DBMSFeatures feat = DBMSResources.instance().getSpecificFeatures(dialect);
		ColumnDiff.updateFeatures(feat);
		log.debug("dialect: "+dialect+" ; feats: "+feat);
		differ.setTypesForDiff("SCHEMA_META,TABLE");
		SchemaDiff diff = differ.diffSchemas(dbModel, model);
		
		// remove unwanted diffs
		diff.getGrantDiffs().clear(); //do not dump Grant diffs
		List<ChangeType> typesToRemove = Arrays.asList(new ChangeType[] {ChangeType.REMARKS, ChangeType.DROP});
		removeDiffsOfTypes(diff.getTableDiffs(), typesToRemove);
		removeDiffsOfTypes(diff.getColumnDiffs(), typesToRemove);
		removeDiffsOfTypes(diff.getDbIdDiffs(), typesToRemove);

		// compact diff
		//SchemaDiff.logInfo(diff);
		diff.compact();
		log.debug("diff: compacted diffs");
		//SchemaDiff.logInfo(diff);
		
		return diff;
	}

	public static void applySchemaDiff(SchemaDiff diff, Connection conn, boolean apply) throws IOException, SQLException {
		// output diff
		int diffcount = diff.getDiffListSize();
		if(!apply) {
			log.info("will not apply diff [apply = "+apply+" ; diffcount = "+diffcount+"]");
			//diff.outDiffs(cout);
		}
		// apply...
		else {
			List<Diff> diffs = diff.getChildren();
			if(diffs.size()>0) {
				log.info("diff: will apply "+diffs.size()+" diffs");
				applyDiffs(diffs, conn);
				log.info("diff: applyed diffs");
			}
			else {
				log.info("no diffs to apply [apply = "+apply+" ; diffcount = "+diffcount+" ; diffs.size() = "+diffs.size()+"]");
			}
		}
		//log.info("diff finished");
	}
	
	public static Set<String> getSchemaNames(Set<? extends DBIdentifiable> dbobjects) {
		Set<String> ret = new HashSet<>();
		for(DBIdentifiable dbid: dbobjects) {
			String sname = dbid.getSchemaName();
			if(sname!=null) {
				ret.add(sname);
			}
		}
		return ret;
	}

}
