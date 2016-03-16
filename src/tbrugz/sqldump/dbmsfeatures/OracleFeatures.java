package tbrugz.sqldump.dbmsfeatures;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Date;
import java.util.List;
import java.util.Properties;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import tbrugz.sqldump.dbmd.AbstractDBMSFeatures;
import tbrugz.sqldump.dbmodel.Column;
import tbrugz.sqldump.dbmodel.Constraint;
import tbrugz.sqldump.dbmodel.DBIdentifiable;
import tbrugz.sqldump.dbmodel.DBObjectType;
import tbrugz.sqldump.dbmodel.ExecutableObject;
import tbrugz.sqldump.dbmodel.ExecutableParameter;
import tbrugz.sqldump.dbmodel.ExecutableParameter.INOUT;
import tbrugz.sqldump.dbmodel.FK;
import tbrugz.sqldump.dbmodel.Grant;
import tbrugz.sqldump.dbmodel.Index;
import tbrugz.sqldump.dbmodel.Index.IndexType;
import tbrugz.sqldump.dbmodel.MaterializedView;
import tbrugz.sqldump.dbmodel.PrivilegeType;
import tbrugz.sqldump.dbmodel.SchemaModel;
import tbrugz.sqldump.dbmodel.Sequence;
import tbrugz.sqldump.dbmodel.Synonym;
import tbrugz.sqldump.dbmodel.Table;
import tbrugz.sqldump.dbmodel.Trigger;
import tbrugz.sqldump.dbmodel.View;
import tbrugz.sqldump.util.SQLUtils;
import tbrugz.sqldump.util.Utils;

public class OracleFeatures extends AbstractDBMSFeatures {
	private static Log log = LogFactory.getLog(OracleFeatures.class);

	public static final String PROP_USE_DBA_METAOBJECTS = PREFIX_DBMS+".oracle.use-dba-metaobjects";
	/*
	 * https://docs.oracle.com/cd/B28359_01/server.111/b28320/statviews_2108.htm
	 * 
	 * ALL_TRIGGERS describes the triggers on tables accessible to the current user. If the user has 
	 * the CREATE ANY TRIGGER privilege, then this view describes all triggers in the database.
	 * 
	 * DBA_TRIGGERS describes all triggers in the database.
	 */
	public static final String PROP_USE_DBA_TRIGGERS = PREFIX_DBMS+".oracle.trigger.use-dba-triggers";
	
	static final DBObjectType[] execTypes = new DBObjectType[]{
		DBObjectType.FUNCTION, DBObjectType.PACKAGE, DBObjectType.PACKAGE_BODY, DBObjectType.PROCEDURE
		//, DBObjectType.TYPE, DBObjectType.TYPE_BODY, DBObjectType.JAVA_SOURCE
	};
	
	//boolean dumpSequenceStartWith = true;
	boolean grabExecutablePrivileges = true; //XXX: add prop for 'grabExecutablePrivileges'?
	static boolean useDbaMetadataObjects = true;
	boolean useDbaTriggers = useDbaMetadataObjects;
	
	@Override
	public void procProperties(Properties prop) {
		super.procProperties(prop);
		//dumpSequenceStartWith = "true".equals(prop.getProperty(PROP_SEQUENCE_STARTWITHDUMP));
		Sequence.dumpStartWith = Utils.getPropBool(prop, PROP_DUMP_SEQUENCE_STARTWITH, Sequence.dumpStartWith);
		OracleDatabaseMetaData.grabFKFromUK = Utils.getPropBool(prop, PROP_GRAB_FKFROMUK, OracleDatabaseMetaData.grabFKFromUK);
		OracleTable.dumpPhysicalAttributes = Utils.getPropBool(prop, PROP_DUMP_TABLE_PHYSICAL_ATTRIBUTES, OracleTable.dumpPhysicalAttributes);
		OracleTable.dumpLoggingClause = Utils.getPropBool(prop, PROP_DUMP_TABLE_LOGGING, OracleTable.dumpLoggingClause);
		OracleTable.dumpPartitionClause = Utils.getPropBool(prop, PROP_DUMP_TABLE_PARTITION, OracleTable.dumpPartitionClause);
		useDbaMetadataObjects = Utils.getPropBool(prop, PROP_USE_DBA_METAOBJECTS, useDbaMetadataObjects);
		useDbaTriggers = Utils.getPropBool(prop, PROP_USE_DBA_TRIGGERS, useDbaTriggers);
	}
	
	/* (non-Javadoc)
	 * @see tbrugz.sqldump.DbmgrFeatures#grabDBObjects(tbrugz.sqldump.SchemaModel, java.lang.String, java.sql.Connection)
	 */
	@Override
	public void grabDBObjects(SchemaModel model, String schemaPattern, Connection conn) throws SQLException {
		if(grabViews) {
			grabDBViews(model.getViews(), schemaPattern, null, conn);
		}
		if(grabMaterializedViews) {
			grabDBMaterializedViews(model.getViews(), schemaPattern, null, conn);
		}
		if(grabTriggers) {
			grabDBTriggers(model.getTriggers(), schemaPattern, null, null, conn);
		}
		if(grabExecutables) {
			grabDBExecutables(model.getExecutables(), schemaPattern, null, conn);
		}
		if(grabSynonyms) {
			grabDBSynonyms(model.getSynonyms(), schemaPattern, null, conn);
		}
		if(grabIndexes) {
			grabDBIndexes(model, schemaPattern, conn);
		}
		if(grabSequences) {
			grabDBSequences(model.getSequences(), schemaPattern, null, conn);
		}
		if(grabCheckConstraints) {
			grabDBCheckConstraints(model.getTables(), schemaPattern, null, conn);
		}
		if(grabUniqueConstraints) {
			grabDBUniqueConstraints(model.getTables(), schemaPattern, null, conn);
		}
	}
	
	String grabDBViewsQuery(String schemaPattern, String viewNamePattern) {
		return "SELECT owner, VIEW_NAME, 'VIEW' as view_type, TEXT"
				+" FROM "+(useDbaMetadataObjects?"DBA_VIEWS ":"ALL_VIEWS ")
				+" where owner = '"+schemaPattern+"'"
				+(viewNamePattern!=null?" and view_name = '"+viewNamePattern+"'":"")
				+ " ORDER BY VIEW_NAME";
	}

	/*@Override
	public void grabDBViews(SchemaModel model, String schemaPattern, String viewNamePattern, Connection conn) throws SQLException {
		grabDBNormalViews(model.getViews(), schemaPattern, viewNamePattern, conn);
		grabDBMaterializedViews(model.getViews(), schemaPattern, viewNamePattern, conn);
	}*/

	@Override
	public void grabDBViews(Collection<View> views, String schemaPattern, String viewNamePattern, Connection conn) throws SQLException {
		grabDBNormalViews(views, schemaPattern, viewNamePattern, conn);
		//grabDBMaterializedViews(views, schemaPattern, viewNamePattern, conn);
	}
	
	/*void grabDBNormalViews(SchemaModel model, String schemaPattern, String viewNamePattern, Connection conn) throws SQLException {
		grabDBNormalViews(model.getViews(), schemaPattern, viewNamePattern, conn);
	}*/

	void grabDBNormalViews(Collection<View> views, String schemaPattern, String viewNamePattern, Connection conn) throws SQLException {
		log.debug("grabbing (normal) views");
		String query = grabDBViewsQuery(schemaPattern, viewNamePattern);
		log.debug("sql: "+query);
		Statement st = conn.createStatement();
		ResultSet rs = st.executeQuery(query);
		
		int count = 0;
		while(rs.next()) {
			View v = new View();
			v.setName( rs.getString(2) );
			v.setQuery( rs.getString(4) );
			v.setSchemaName(schemaPattern);
			views.add(v);
			count++;
		}
		rs.close();
		st.close();
		
		log.info("["+schemaPattern+"]: "+count+" views grabbed");// ["+model.views.size()+"/"+count+"]: ");
	}

	String grabDBMaterializedViewsQuery(String schemaPattern, String viewNamePattern) {
		return "select owner, mview_name, 'MATERIALIZED_VIEW' AS VIEW_TYPE, query, "
				+" rewrite_enabled, rewrite_capability, refresh_mode, refresh_method, build_mode, fast_refreshable "
				+" from "+(useDbaMetadataObjects?"dba_mviews ":"all_mviews ")
				+" where owner = '"+schemaPattern+"' "
				+(viewNamePattern!=null?" and mview_name = '"+viewNamePattern+"' ":"")
				+"ORDER BY MVIEW_NAME";
	}

	/*void grabDBMaterializedViews(SchemaModel model, String schemaPattern, String viewNamePattern, Connection conn) throws SQLException {
		grabDBMaterializedViews(model.getViews(), schemaPattern, viewNamePattern, conn);
	}*/

	@Override
	public void grabDBMaterializedViews(Collection<View> views, String schemaPattern, String viewNamePattern, Connection conn) throws SQLException {
		log.debug("grabbing materialized views");
		String query = grabDBMaterializedViewsQuery(schemaPattern, viewNamePattern);
		log.debug("sql: "+query);
		Statement st = conn.createStatement();
		ResultSet rs = st.executeQuery(query);
		
		int count = 0;
		while(rs.next()) {
			MaterializedView v = new MaterializedView();
			v.setName( rs.getString(2) );
			v.setQuery( rs.getString(4) );
			v.setSchemaName(schemaPattern);
			v.rewriteEnabled = "Y".equals(rs.getString(5));
			v.rewriteCapability = rs.getString(6);
			v.refreshMode = rs.getString(7);
			v.refreshMethod = rs.getString(8);
			views.add(v);
			count++;
		}
		rs.close();
		st.close();
		
		log.info("["+schemaPattern+"]: "+count+" materialized views grabbed");
	}

	String grabDBTriggersQuery(String schemaPattern, String tableNamePattern, String triggerNamePattern) {
		return "SELECT TRIGGER_NAME, TABLE_OWNER, TABLE_NAME, DESCRIPTION, TRIGGER_BODY, WHEN_CLAUSE "
				+"FROM "+(useDbaTriggers?"DBA_TRIGGERS ":"ALL_TRIGGERS ")
				+"where owner = '"+schemaPattern+"' "
				+(tableNamePattern!=null?" and table_name = '"+tableNamePattern+"' ":"")
				+(triggerNamePattern!=null?" and trigger_name = '"+triggerNamePattern+"' ":"")
				//+"and status = 'ENABLED' "
				+"ORDER BY trigger_name";
	}
	
	/*@Override
	public void grabDBTriggers(SchemaModel model, String schemaPattern, String tableNamePattern, String triggerNamePattern, Connection conn) throws SQLException {
		grabDBTriggers(model.getTriggers(), schemaPattern, tableNamePattern, triggerNamePattern, conn);
	}*/
	
	@Override
	public void grabDBTriggers(Collection<Trigger> triggers, String schemaPattern, String tableNamePattern, String triggerNamePattern, Connection conn) throws SQLException {
		log.debug("grabbing triggers");
		String query = grabDBTriggersQuery(schemaPattern, tableNamePattern, triggerNamePattern);
		log.debug("sql: "+query);
		Statement st = conn.createStatement();
		ResultSet rs = st.executeQuery(query);
		
		int count = 0;
		while(rs.next()) {
			Trigger t = new Trigger();
			t.setName( rs.getString(1) );
			t.setSchemaName(rs.getString(2));
			t.setTableName(rs.getString(3));
			String description = rs.getString(4);
			if(description!=null) { t.setDescription(description.trim()); }
			t.setBody(rs.getString(5));
			t.setWhenClause(rs.getString(6));
			
			triggers.add(t);
			count++;
		}
		rs.close();
		st.close();
		
		log.info("["+schemaPattern+"]: "+count+" triggers grabbed");
	}
	
	String grabDBExecutablesQuery(String schemaPattern, String execNamePattern) {
		return "select name, type, line, text "
				+"from all_source " // (useDbaMetadataObjects?"dba_source ":"all_source ")
				+"where type in ('PROCEDURE','PACKAGE','PACKAGE BODY','FUNCTION','TYPE') "
				+"and owner = '"+schemaPattern+"' "
				+(execNamePattern!=null?" and name = '"+execNamePattern+"' ":"")
				+"order by type, name, line";
	}
	
	/*@Override
	public void grabDBExecutables(SchemaModel model, String schemaPattern, String execNamePattern, Connection conn) throws SQLException {
		grabDBExecutables(model.getExecutables(), schemaPattern, execNamePattern, conn);
	}*/
	
	@Override
	public void grabDBExecutables(Collection<ExecutableObject> execs, String schemaPattern, String execNamePattern, Connection conn) throws SQLException {
		log.debug("grabbing executables");
		String query = grabDBExecutablesQuery(schemaPattern, execNamePattern);
		log.debug("sql: "+query);
		Statement st = conn.createStatement();
		ResultSet rs = st.executeQuery(query);
		
		int linecount = 0;
		int countExecutables = 0;
		ExecutableObject eo = null;
		StringBuilder sb = null;
		
		while(rs.next()) {
			int line = rs.getInt(3);
			if(line==1) {
				//end last object
				if(eo!=null) {
					eo.setBody( sb.toString() );
					boolean added = addExecutableToModel(execs, eo);
					if(added) { countExecutables++; }
				}
				//new object
				eo = new ExecutableObject();
				sb = new StringBuilder();
				eo.setName( rs.getString(1) );
				try {
					eo.setType( DBObjectType.valueOf(Utils.normalizeEnumStringConstant(rs.getString(2))) );
				}
				catch(IllegalArgumentException iae) {
					log.warn("unknown object type: "+rs.getString(2));
					eo.setType( DBObjectType.EXECUTABLE );
				}
				
				eo.setSchemaName(schemaPattern);
				
				if(grabExecutablePrivileges && !eo.getType().equals(DBObjectType.PACKAGE_BODY)) {
					//XXX: optimize it: make one query instead of many
					grabExecutablePrivileges(eo, schemaPattern, conn);
				}
			}
			sb.append(rs.getString(4)); //+"\n"
			linecount++;
		}
		if(sb!=null) {
			eo.setBody( sb.toString() );
			boolean added = addExecutableToModel(execs, eo);
			if(added) { countExecutables++; }
		}
		rs.close();
		st.close();
		
		log.info("["+schemaPattern+(execNamePattern!=null?"."+execNamePattern:"")+"]: "+countExecutables+" executable objects grabbed [linecount="+linecount+"]");
		
		//grabs metadata
		grabDBExecutablesMetadata(execs, schemaPattern, execNamePattern, conn);
	}
	
	boolean addExecutableToModel(Collection<ExecutableObject> execs, ExecutableObject eo) {
		boolean added = execs.add(eo);
		if(!added) {
			boolean b1 = execs.remove(eo);
			boolean b2 = execs.add(eo);
			added = b1 && b2;
			if(added) {
				log.debug("executable ["+eo.getType()+"] '"+eo.getQualifiedName()+"' replaced in model");
			}
			else {
				log.warn("executable ["+eo.getType()+"] '"+eo.getQualifiedName()+"' not added to model [removed: "+b1+" ; added: "+b2+"]");
			}
		}
		return added;
	}
	
	void grabDBExecutablesMetadata(Collection<ExecutableObject> execs, String schemaPattern, String execNamePattern, Connection conn) throws SQLException {
		String query = "select p.owner, p.object_id, p.object_name, p.subprogram_id, p.procedure_name, p.object_type, "
				+"       (select case min(position) when 0 then 'FUNCTION' when 1 then 'PROCEDURE' end from all_arguments aaz where p.object_id = aaz.object_id and p.subprogram_id = aaz.subprogram_id) as subprogram_type, "
				+"       aa.argument_name, aa.position, aa.sequence, aa.data_type, aa.in_out, aa.data_length, aa.data_precision, aa.data_scale, aa.pls_type "
				+"  from all_procedures p "
				+"  left outer join all_arguments aa on p.object_id = aa.object_id and p.subprogram_id = aa.subprogram_id "
				+" where p.owner = '"+schemaPattern+"' "
				+(execNamePattern!=null?" and p.object_name = '"+execNamePattern+"' ":"")
				//+"   and p.object_type = 'PACKAGE' "
				//+"   and p.procedure_name is not null "
				+"   and aa.position is not null "
				+" order by p.owner, p.object_name, p.subprogram_id, procedure_name, aa.position ";
		log.debug("grabbing executables' metadata - sql: "+query);
		Statement st = conn.createStatement();
		ResultSet rs = st.executeQuery(query);

		ExecutableObject eo = null;
		//int executablesCount = 0;
		int newExecutablesCount = 0;
		int paramCount = 0;
		
		while(rs.next()) {
			String objectName = rs.getString(3);
			String subprogramName = rs.getString(5);
			String objectType = rs.getString(6);
			String subprogramType = rs.getString(7);
			log.debug("subprogram: "+subprogramName+" ; type="+subprogramType+" -- objName/package="+objectName+" ; objType="+objectType);
			
			//is a procedure?
			eo = DBIdentifiable.getDBIdentifiableByTypeSchemaAndName(execs, DBObjectType.PROCEDURE, schemaPattern, objectName);
			if(eo==null) { //not a procedure, maybe a function
				eo = DBIdentifiable.getDBIdentifiableByTypeSchemaAndName(execs, DBObjectType.FUNCTION, schemaPattern, objectName);
			}
			//not a top-level procedure or function, maybe declared inside a package
			if(eo==null && subprogramName!=null) {
				eo = DBIdentifiable.getDBIdentifiableByTypeSchemaAndName(execs, DBObjectType.PROCEDURE, schemaPattern, subprogramName);
			}
			if(eo==null && subprogramName!=null) {
				eo = DBIdentifiable.getDBIdentifiableByTypeSchemaAndName(execs, DBObjectType.FUNCTION, schemaPattern, subprogramName);
			}
			
			//not a procedure or function, maybe a package (remember packages have no parameters)
			/*if(eo==null) {
				eo = DBIdentifiable.getDBIdentifiableByTypeSchemaAndName(model.getExecutables(), DBObjectType.PACKAGE, schemaPattern, objectName);
				log.info("searching for package: "+subprogramName+" / type="+subprogramType+" /// objName="+objectName+" / objType="+objectType+" --> "+eo);
			}*/
			
			//not found, let's create it
			if(eo==null) {
				if(subprogramName==null) {
					log.warn("subprogram is null. object="+objectName+" / type="+objectType+" / schema="+schemaPattern);
					continue;
				}
				log.debug("new Executable: subprogram: "+subprogramName+" / type="+subprogramType+" /// objName="+objectName+" / objType="+objectType);
				
				eo = new ExecutableObject();
				eo.setSchemaName(schemaPattern);
				
				DBObjectType otype = DBObjectType.valueOf(objectType);
				switch(otype) {
				case PACKAGE:
					eo.setPackageName(objectName);
					eo.setName(subprogramName);
					eo.setType(DBObjectType.valueOf(subprogramType));
					break;
				default: //top-level procedure or function
					eo.setName(objectName);
					eo.setType(otype);
				}
				
				execs.add(eo);
				newExecutablesCount++;
			}
			else {
				log.debug("procedure or function found: "+objectName+" / "+subprogramName+" / "+objectType+" / eo="+eo);
			}
			ExecutableParameter ep = new ExecutableParameter();
			ep.setName(rs.getString(8));
			ep.setDataType(rs.getString(11));
			ep.setPosition(rs.getInt(9));
			String inout = rs.getString(12);
			if(inout!=null) {
				try {
					ep.setInout(INOUT.getValue(inout));
				}
				catch(IllegalArgumentException e) {
					log.warn("illegal INOUT value: "+inout);
				}
			}
			//log.info("parameter: "+ep);
			if(ep.getPosition()==0) {
				eo.setReturnParam(ep);
			}
			else {
				if(eo.getParams()==null) {
					eo.setParams(new ArrayList<ExecutableParameter>());
				}
				eo.getParams().add(ep);
			}
			paramCount++;
		}

		log.info("["+schemaPattern+(execNamePattern!=null?"."+execNamePattern:"")+"]: "+newExecutablesCount+" xtra executables grabbed (metadata); "+paramCount+" executable's parameters grabbed");
	}
	
	void grabExecutablePrivileges(ExecutableObject executable, String schemaPattern, Connection conn) throws SQLException {
		String sql = "SELECT grantee, grantable FROM all_tab_privs WHERE table_schema = ? AND table_name = ? AND privilege = 'EXECUTE'";
		log.debug("sql: "+sql);
		PreparedStatement st = conn.prepareStatement(sql);
		st.setString(1, schemaPattern);
		st.setString(2, executable.getName());
		ResultSet rs = st.executeQuery();
		
		while(rs.next()) {
			String grantee = rs.getString(1);
			boolean grantable = "YES".equals(rs.getString(2));
			
			Grant grant = new Grant();
			grant.setGrantee(grantee);
			grant.setPrivilege(PrivilegeType.EXECUTE);
			grant.setTable(executable.getName());
			grant.setWithGrantOption(grantable);
			
			executable.getGrants().add(grant);
		}
	}
	
	/*public List<NamedDBObject> grabExecutableNames(String catalog, String schema, String executableNamePattern, Connection conn) throws SQLException {
		String sql = "select distinct name, type from all_source "
				+"where type in ('PROCEDURE','PACKAGE','PACKAGE BODY','FUNCTION','TYPE') "
				+"and owner = ? "
				+"order by type, name";
		PreparedStatement st = conn.prepareStatement(sql);
		st.setString(1, schema);
		ResultSet rs = st.executeQuery();
		
		while(rs.next()) {
			String name = rs.getString(1);
		}

	}*/
	
	String grabDBSynonymsQuery(String schemaPattern, String synonymNamePattern) {
		return "select synonym_name, table_owner, table_name, db_link "
				+"from "+(useDbaMetadataObjects?"dba_synonyms ":"all_synonyms ")
				+"where owner = '"+schemaPattern+"'"
				+(synonymNamePattern!=null?" and synonym_name = '"+synonymNamePattern+"'":"");
	}
	
	/*@Override
	public void grabDBSynonyms(SchemaModel model, String schemaPattern, String synonymNamePattern, Connection conn) throws SQLException {
		grabDBSynonyms(model.getSynonyms(), schemaPattern, synonymNamePattern, conn);
	}*/
	
	@Override
	public void grabDBSynonyms(Collection<Synonym> synonyms, String schemaPattern, String synonymNamePattern, Connection conn) throws SQLException {
		log.debug("grabbing synonyms");
		String query = grabDBSynonymsQuery(schemaPattern, synonymNamePattern);
		log.debug("sql: "+query);
		Statement st = conn.createStatement();
		ResultSet rs = st.executeQuery(query);
		
		int count = 0;
		while(rs.next()) {
			Synonym s = new Synonym();
			s.setName( rs.getString(1) );
			s.setSchemaName(schemaPattern);
			s.setObjectOwner( rs.getString(2) );
			s.setReferencedObject( rs.getString(3) );
			s.setDbLink( rs.getString(4) );
			synonyms.add(s);
			count++;
		}
		rs.close();
		st.close();
		
		log.info("["+schemaPattern+"]: "+count+" synonyms grabbed");
	}

	String grabDBIndexesQuery(String schemaPattern) {
		return "select ui.table_owner, ui.index_name, ui.uniqueness, ui.index_type, ui.table_name, uic.column_name, uic.column_position, uip.partitioning_type, uip.locality, uie.column_expression "
				+"from all_indexes ui, all_ind_columns uic, all_part_indexes uip, all_ind_expressions uie "
				+"where UI.INDEX_NAME = UIC.INDEX_NAME "
				+"and ui.table_name = uic.table_name "
				+"and ui.table_owner = uic.table_owner "
				+"and ui.index_name = uip.index_name (+) "
				+"and ui.table_name = uip.table_name (+) "
				+"and ui.table_owner = uip.owner (+) "
				+"and uic.index_name = uie.index_name (+) "
				+"and uic.table_name = uie.table_name (+) "
				+"and uic.table_owner = uie.table_owner (+) "
				+"and uic.column_position = uie.column_position (+) "
				+"and ui.owner = '"+schemaPattern+"' "
				+"order by ui.table_owner, ui.index_name, uic.column_position, uie.column_position ";
	}
	
	/*
	 * TODO: move to OracleDatabaseMetaData ?
	 */
	void grabDBIndexes(SchemaModel model, String schemaPattern, Connection conn) throws SQLException {
		log.debug("grabbing indexes");
		/*
		select * from user_indexes   
		select * from user_ind_columns
		select * from user_ind_expressions
		select * from user_join_ind_columns

		select * from all_objects where object_name like 'USER_%'

		or:
		select dbms_metadata.get_ddl('INDEX',index_name) from user_indexes
		see: http://www.dba-oracle.com/concepts/creating_indexes.htm
		*/
		String query = grabDBIndexesQuery(schemaPattern);
		log.debug("sql: "+query);
		Statement st = conn.createStatement();
		ResultSet rs = st.executeQuery(query);
		
		int colCount = 0;
		int idxCount = 0;
		Index idx = null;
		
		while(rs.next()) {
			String idxName = rs.getString(2);
			//if first (idx==null) or new index
			if(idx==null || !idxName.equals(idx.getName())) {
				//end last object
				if(idx!=null) {
					boolean added = addIndexToModel(model, idx);
					if(added) { idxCount++; }
				}
				//new object
				idx = new Index();
				idx.setName( idxName );
				idx.setUnique(rs.getString(3).equals("UNIQUE"));
				idx.setSchemaName(rs.getString(1));
				setIndexType(idx, rs.getString(4));
				idx.setTableName(rs.getString(5));
				if("LOCAL".equals(rs.getString("LOCALITY"))) {
					idx.setLocal(true);
				}
			}
			if(IndexType.FUNCTION_BASED_NORMAL.equals(idx.getIndexType())) {
				idx.getColumns().add(rs.getString("COLUMN_EXPRESSION"));
			}
			else {
				idx.getColumns().add(rs.getString("COLUMN_NAME"));
			}
			colCount++;
		}
		if(idx!=null) {
			boolean added = addIndexToModel(model, idx);
			if(added) { idxCount++; }
		}
		rs.close();
		st.close();
		
		log.info("["+schemaPattern+"]: "+idxCount+" indexes grabbed [colcount="+colCount+"]");
	}
	
	boolean grabIndexesFromUnkownTables = false;
	boolean addIndexToModel(SchemaModel model, Index idx) {
		if(!grabIndexesFromUnkownTables) {
			Table t = DBIdentifiable.getDBIdentifiableByTypeSchemaAndName(model.getTables(), DBObjectType.TABLE, idx.getSchemaName(), idx.getTableName());
			if(t==null) {
				log.debug("table '"+idx.getSchemaName()+"."+idx.getTableName()+"' not found in model, index "+idx.getName()+" won't be grabbed");
				return false;
			}
		}
		return model.getIndexes().add(idx);
	}
	
	/*
	 * about oracle index types:
	 * NORMAL
	 * IOT - TOP -- http://docs.oracle.com/cd/B28359_01/server.111/b28310/tables012.htm#ADMIN11685
	 * BITMAP
	 * FUNCTION-BASED NORMAL - https://docs.oracle.com/cd/E11882_01/appdev.112/e41502/adfns_indexes.htm#ADFNS00505
	 * DOMAIN - http://docs.oracle.com/cd/B10501_01/appdev.920/a96595/dci07idx.htm
	 * NORMAL/REV
	 * ? FUNCTION-BASED DOMAIN 
	 */
	static void setIndexType(Index idx, String typeStr) {
		if(typeStr==null) { return; }
		if(typeStr.equals("NORMAL")) { return; }
		if(typeStr.equals("BITMAP")) { idx.setType(typeStr); return; }
		if(typeStr.equals("NORMAL/REV")) { idx.setReverse(true); return; }
		if(typeStr.equals("FUNCTION-BASED NORMAL")) {
			idx.setIndexType(IndexType.FUNCTION_BASED_NORMAL);
			idx.setComment("FUNCTION-BASED NORMAL index - experimental");
			return;
		}
		//XXX: 'unknown' index types: DOMAIN, FUNCTION-BASED DOMAIN, IOT - TOP
		idx.setComment("unknown index type: '"+typeStr+"'");
	}

	String grabDBSequencesQuery(String schemaPattern, String sequenceNamePattern) {
		return "select sequence_name, min_value, increment_by, last_number "
				+"from "+(useDbaMetadataObjects?"dba_sequences ":"all_sequences ")
				+"where sequence_owner = '"+schemaPattern+"' "
				+(sequenceNamePattern!=null?" and sequence_name = '"+sequenceNamePattern+"'":"")
				+"order by sequence_name";
	}
	
	/*@Override
	public void grabDBSequences(SchemaModel model, String schemaPattern, String sequenceNamePattern, Connection conn) throws SQLException {
		grabDBSequences(model.getSequences(), schemaPattern, sequenceNamePattern, conn);
	}*/
	
	@Override
	public void grabDBSequences(Collection<Sequence> seqs, String schemaPattern, String sequenceNamePattern, Connection conn) throws SQLException {
		log.debug("grabbing sequences");
		String query = grabDBSequencesQuery(schemaPattern, sequenceNamePattern);
		log.debug("sql: "+query);
		Statement st = conn.createStatement();
		ResultSet rs = st.executeQuery(query);
		
		int count = 0;
		while(rs.next()) {
			Sequence s = new Sequence();
			s.setName( rs.getString(1) );
			s.setSchemaName(schemaPattern);
			s.setMinValue(rs.getLong(2));
			s.setIncrementBy(rs.getLong(3));
			s.setLastNumber(rs.getLong(4));
			seqs.add(s);
			count++;
		}
		rs.close();
		st.close();
		
		log.info("["+schemaPattern+"]: "+count+" sequences grabbed");
	}
	
	@Override
	public DatabaseMetaData getMetadataDecorator(DatabaseMetaData metadata) {
		return new OracleDatabaseMetaData(metadata);
	}
	
	@Override
	public Table getTableObject() {
		return new OracleTable();
	}
	
	@Override
	public void addTableSpecificFeatures(Table t, ResultSet rs) {
		String columnName = null;
		if(t instanceof OracleTable) {
			OracleTable ot = (OracleTable) t;
			try {
				columnName = "TABLESPACE_NAME";
				ot.tableSpace = rs.getString(columnName);
				columnName = "TEMPORARY";
				ot.temporary = "YES".equals(rs.getString(columnName));
				columnName = "LOGGING";
				ot.logging = "YES".equals(rs.getString(columnName));
				
				// partition...
				columnName = "PARTITIONED";
				if("YES".equals(rs.getString(columnName))) {
					ot.partitioned = true;
					columnName = "PARTITIONING_TYPE";
					ot.partitionType = OracleTable.PartitionType.valueOf(rs.getString(columnName));
					
					//get partition type, columns
					getPartitionColumns(ot, rs.getStatement().getConnection());
					//get partitions
					getPartitions(ot, rs.getStatement().getConnection());
				}
			}
			catch(SQLException e) {
				//try {
					//log.warn("OracleSpecific: "+e+"; col names: ("+SQLUtils.getColumnNames(rs.getMetaData())+")");
					log.warn("OracleSpecific [lastcol="+columnName+"]: "+e);
				/*} catch (SQLException e1) {
					e1.printStackTrace();
				}*/
				//e.printStackTrace();
			}
		}
		else {
			log.warn("Table "+t+" should be instance of OracleTable");
		}
	}
	
	@Override
	public void addColumnSpecificFeatures(Column c, ResultSet rs) {
		try {
			String dataDefault = rs.getString("DATA_DEFAULT");
			//String comments = rs.getString("REMARKS");
			if(dataDefault!=null) {
				c.setDefaultValue(dataDefault.trim());
			}
			//if(comments!=null) {
			//	c.setRemarks(comments.trim());
			//}
		} catch (SQLException e) {
			log.warn("resultset has no 'DATA_DEFAULT'(?); column: '"+c+"', message: '"+e.getMessage()+"'");
			log.debug("sql exception:", e);
		}
	}

	/*@Override
	public void grabDBCheckConstraints(SchemaModel model, String schemaPattern, String constraintNamePattern, Connection conn) throws SQLException {
		grabDBCheckConstraints(model.getTables(), schemaPattern, constraintNamePattern, conn);
	}*/
	
	@Override
	public void grabDBCheckConstraints(Collection<Table> tables, String schemaPattern, String constraintNamePattern, Connection conn) throws SQLException {
		log.debug("grabbing check constraints");
		
		//check constraints
		String query = "select owner, table_name, constraint_name, constraint_type, search_condition "
				+"from "+(useDbaMetadataObjects?"dba_constraints ":"all_constraints ")
				+"where owner = '"+schemaPattern+"' "
				+(constraintNamePattern!=null?"and constraint_name = '"+constraintNamePattern+"' ":"")
				+"and constraint_type = 'C' "
				+"order by owner, table_name, constraint_name ";
		Statement st = conn.createStatement();
		log.debug("sql: "+query);
		ResultSet rs = st.executeQuery(query);
		
		int count = 0;
		int countNotAdded = 0;
		while(rs.next()) {
			Constraint c = new Constraint();
			String tableName = rs.getString(2);
			c.setType(Constraint.ConstraintType.CHECK);
			c.setName( rs.getString(3) );
			c.setCheckDescription(rs.getString(5));
			
			//ignore NOT NULL constraints
			if(c.getCheckDescription().contains(" IS NOT NULL")) { continue; }
			
			Table t = DBIdentifiable.getDBIdentifiableBySchemaAndName(tables, rs.getString(1), tableName);
			if(t!=null) {
				t.getConstraints().add(c);
				count++;
			}
			else {
				countNotAdded++;
				if(c.getName()==null || c.getName().startsWith("BIN$")) {
					log.debug("deleted check constraint "+c+" can't be added to table '"+tableName+"': table not found");
				}
				else {
					log.warn("check constraint "+c+" can't be added to table '"+tableName+"': table not found");
				}
			}
		}
		rs.close();
		st.close();
		
		log.info("["+schemaPattern+"]: "+count+" check constraints grabbed"+
				(countNotAdded>0?" ["+countNotAdded+" constraints ignored]":"")
				);
	}

	@Override
	public void grabDBUniqueConstraints(Collection<Table> tables, String schemaPattern, String constraintNamePattern, Connection conn) throws SQLException {
		log.debug("grabbing unique constraints");

		//unique constraints
		String query = "select distinct al.owner, al.table_name, al.constraint_name, column_name, position "
				+"from "+(useDbaMetadataObjects?"dba_constraints al, dba_cons_columns acc ":"all_constraints al, all_cons_columns acc ")
				+"where al.constraint_name = acc.constraint_name "
				+"and al.owner = '"+schemaPattern+"' "
				+(constraintNamePattern!=null?"and al.constraint_name = '"+constraintNamePattern+"' ":"")
				+"and constraint_type = 'U' "
				+"order by owner, table_name, constraint_name, position, column_name ";
		Statement st = conn.createStatement();
		log.debug("sql: "+query);
		ResultSet rs = st.executeQuery(query);
		
		int colCount = 0;
		int countNotAdded = 0;
		int countUniqueConstraints = 0;
		String previousConstraint = null;
		Constraint c = null;
		while(rs.next()) {
			String constraintName = rs.getString(3);
			if(!constraintName.equals(previousConstraint)) {
				String tableName = rs.getString(2);
				c = new Constraint();
				Table t = DBIdentifiable.getDBIdentifiableBySchemaAndName(tables, rs.getString(1), tableName);
				if(t!=null) {
					t.getConstraints().add(c);
					countUniqueConstraints++;
				}
				else {
					if(c.getName()==null || c.getName().startsWith("BIN$")) {
						log.debug("deleted unique constraint "+c+" can't be added to table '"+tableName+"': table not found");
					}
					else {
						log.warn("unique constraint "+c+" can't be added to table '"+tableName+"': table not found");
					}
					countNotAdded++;
				}
				c.setType(Constraint.ConstraintType.UNIQUE);
				c.setName( constraintName );
			}
			c.getUniqueColumns().add(rs.getString(4));
			previousConstraint = constraintName;
			colCount++;
		}
		rs.close();
		st.close();
		
		log.info("["+schemaPattern+"]: "+countUniqueConstraints+" unique constraints grabbed"+
				(countNotAdded>0?" ["+countNotAdded+" constraints ignored]":"")+
				" [colcount="+colCount+"]"
				);
	}
	
	void getPartitionColumns(OracleTable ot, Connection conn) throws SQLException {
		String query = "select column_name, column_position "
				+"from all_part_key_columns "
				+"where object_type = 'TABLE' "
				+"and owner = '"+ot.getSchemaName()+"' "
				+"and name = '"+ot.getName()+"' "
				+"order by name, column_position ";
		
		Statement st = conn.createStatement();
		log.debug("sql: "+query);
		ResultSet rs = st.executeQuery(query);

		List<String> columns = new ArrayList<String>();
		while(rs.next()) {
			String col = rs.getString(1);
			columns.add(col);
		}
		ot.partitionColumns = columns;
	}
	
	void getPartitions(OracleTable ot, Connection conn) throws SQLException {
		String query = "select table_owner, table_name, partition_name, partition_position, tablespace_name, high_value, high_value_length "
				+"from all_tab_partitions "
				+"where table_owner = '"+ot.getSchemaName()+"' "
				+"and table_name = '"+ot.getName()+"' "
				+"order by table_name, partition_position ";
		
		Statement st = conn.createStatement();
		log.debug("sql: "+query);
		ResultSet rs = st.executeQuery(query);

		List<OracleTablePartition> parts = new ArrayList<OracleTablePartition>();
		while(rs.next()) {
			OracleTablePartition otp = new OracleTablePartition();
			otp.name = rs.getString("PARTITION_NAME");
			otp.tableSpace = rs.getString("TABLESPACE_NAME");
			switch (ot.partitionType) {
				case RANGE:
					String highValues = rs.getString("HIGH_VALUE");
					String[] hv = highValues.split(",");
					otp.upperValues = Arrays.asList(hv);
					break;
				case LIST:
					String values = rs.getString("HIGH_VALUE");
					String[] v = values.split(",");
					otp.values = Arrays.asList(v);
					break;
				case HASH:
					break;
				default:
					throw new RuntimeException("Unknown partition type: "+ot.partitionType);
			}
			parts.add(otp);
		}
		
		/*if(ot.partitionType==OracleTable.PartitionType.HASH) {
			ot.numberOfPartitions = parts.size();
		}*/
		//ot.numberOfPartitions = parts.size();
		ot.partitions = parts;
	}
	
	@Override
	public FK getForeignKeyObject() {
		return new OracleFK();
	}
	
	boolean grabXtraFKColumns = true;
	
	@Override
	public void addFKSpecificFeatures(FK fk, ResultSet rs) {
		if(fk instanceof OracleFK) {
			try {
				OracleFK ofk = (OracleFK) fk;
				if(grabXtraFKColumns) {
					// acfk.status, acfk.validated, acfk.rely
					String status = rs.getString("STATUS");
					if(status!=null && status.equals("DISABLED")) {
						ofk.enabled = false; 
					}
					String validated = rs.getString("VALIDATED");
					if(validated!=null && validated.equals("NOT VALIDATED")) {
						ofk.validated = false; 
					}
					String rely = rs.getString("RELY");
					if(rely!=null && !rely.equals("")) {
						ofk.rely = false; 
					}
				}
				//log.info("OracleFK: "+ofk);
			} catch (SQLException e) {
				grabXtraFKColumns = false;
				log.warn("addFKSpecificFeatures: column 'STATUS', 'VALIDATED' or 'RELY' not available [ex: "+e+"]");
				if(log.isDebugEnabled()) {
					try { log.debug("rowlist: "+SQLUtils.getColumnNames(rs.getMetaData()));	}
					catch(SQLException ee) { ee.printStackTrace(); }
				}
			}
		}
		else {
			log.warn("FK "+fk+" should be instance of "+OracleFK.class.getSimpleName());
		}
	}
	
	@Override
	public String sqlAddColumnClause() {
		return "add";
	}

	@Override
	public String sqlAlterColumnClause() {
		return "modify";
	}
	
	@Override
	public String sqlDefaultDateFormatPattern() {
		return "'TO_DATE('''yyyy-MM-dd''',''YYYY-MM-DD'')'";
	}

	@Override
	public List<DBObjectType> getExecutableObjectTypes() {
		return Arrays.asList(execTypes);
	}

	/*
	@Override
	public boolean supportsGrabViews() {
		return true;
	}

	@Override
	public boolean supportsGrabTriggers() {
		return true;
	}

	@Override
	public boolean supportsGrabExecutables() {
		return true;
	}

	@Override
	public boolean supportsGrabSequences() {
		return true;
	}

	@Override
	public boolean supportsGrabSynonyms() {
		return true;
	}

	@Override
	public boolean supportsGrabCheckConstraints() {
		return true;
	}

	@Override
	public boolean supportsGrabUniqueConstraints() {
		return true;
	}
	*/
	
	@Override
	public boolean supportsExplainPlan() {
		return true;
	}
	
	final static String PADDING = ".";
	final static String DEFAULT_EXPLAIN_COLUMNS = "ID, PARENT_ID, DEPTH, /*lpad('.', level, '.' ) || OPERATION as OPERATION,*/ lpad('"+PADDING+"', (depth-1)*3, '"+PADDING+"' ) || OPERATION as OPERATION, OPTIONS, OBJECT_OWNER, OBJECT_NAME, /* OBJECT_ALIAS, OBJECT_INSTANCE, */ OBJECT_TYPE, OPTIMIZER, SEARCH_COLUMNS, /*POSITION,*/ COST, CARDINALITY, BYTES, CPU_COST, IO_COST, TIME";
	
	/*
	 * see: https://docs.oracle.com/cd/B28359_01/server.111/b28274/ex_plan.htm#i16938
	 * columns: STATEMENT_ID | PLAN_ID | TIMESTAMP | REMARKS | OPERATION | OPTIONS | OBJECT_NODE | OBJECT_OWNER | OBJECT_NAME | OBJECT_ALIAS | OBJECT_INSTANCE | OBJECT_TYPE | OPTIMIZER | SEARCH_COLUMNS | ID | PARENT_ID | DEPTH | POSITION | COST | CARDINALITY | BYTES | OTHER_TAG | PARTITION_START | PARTITION_STOP | PARTITION_ID | OTHER | OTHER_XML | DISTRIBUTION | CPU_COST | IO_COST | TEMP_SPACE | ACCESS_PREDICATES | FILTER_PREDICATES | PROJECTION | TIME | QBLOCK_NAME
	 */
	@Override
	public ResultSet explainPlan(String sql, Connection conn) throws SQLException {
		Date now = new Date();
		DateFormat df = new SimpleDateFormat("yyyy-MM-dd_HH:mm:ss");
		String id = "sqldump_"+df.format(now); //XXX: add random?
		String planTable = "PLAN_TABLE";
		
		String explainSql = "explain plan\n\tset STATEMENT_ID = '"+id+"' into "+planTable+"\n"
			+ "for\n"+sql;
		//log.debug("explain sql:\n"+explainSql);
		Statement stmt = conn.createStatement();
		stmt.execute(explainSql);
		stmt.close();
		
		String planTableSelect = "select "+DEFAULT_EXPLAIN_COLUMNS
				+"\nfrom "+planTable
				+"\nconnect by prior id = parent_id "
				+"\nand prior statement_id = statement_id "
				+"\nstart with parent_id = 0 "
				+"\nand statement_id = '"+id+"' "
				+"\norder by id ";
		//log.debug("plan_table sql:\n"+explainSql);
		return conn.createStatement().executeQuery(planTableSelect);
	}

}
