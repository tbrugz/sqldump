package tbrugz.sqldump.dbmsfeatures;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import tbrugz.sqldump.SQLUtils;
import tbrugz.sqldump.dbmodel.Column;
import tbrugz.sqldump.dbmodel.Constraint;
import tbrugz.sqldump.dbmodel.DBIdentifiable;
import tbrugz.sqldump.dbmodel.DBObject;
import tbrugz.sqldump.dbmodel.DBObjectType;
import tbrugz.sqldump.dbmodel.ExecutableObject;
import tbrugz.sqldump.dbmodel.ExecutableParameter;
import tbrugz.sqldump.dbmodel.ExecutableParameter.INOUT;
import tbrugz.sqldump.dbmodel.FK;
import tbrugz.sqldump.dbmodel.Grant;
import tbrugz.sqldump.dbmodel.Index;
import tbrugz.sqldump.dbmodel.MaterializedView;
import tbrugz.sqldump.dbmodel.PrivilegeType;
import tbrugz.sqldump.dbmodel.SchemaModel;
import tbrugz.sqldump.dbmodel.Sequence;
import tbrugz.sqldump.dbmodel.Synonym;
import tbrugz.sqldump.dbmodel.Table;
import tbrugz.sqldump.dbmodel.Trigger;
import tbrugz.sqldump.dbmodel.View;
import tbrugz.sqldump.def.AbstractDBMSFeatures;
import tbrugz.sqldump.util.Utils;

public class OracleFeatures extends AbstractDBMSFeatures {
	static Log log = LogFactory.getLog(OracleFeatures.class);

	boolean dumpSequenceStartWith = true;
	boolean grabExecutablePrivileges = true; //XXX: add prop for 'grabExecutablePrivileges'?
	
	@Override
	public void procProperties(Properties prop) {
		super.procProperties(prop);
		//dumpSequenceStartWith = "true".equals(prop.getProperty(PROP_SEQUENCE_STARTWITHDUMP));
		Sequence.dumpStartWith = Utils.getPropBool(prop, PROP_DUMP_SEQUENCE_STARTWITH, Sequence.dumpStartWith);
		OracleDatabaseMetaData.grabFKFromUK = Utils.getPropBool(prop, PROP_GRAB_FKFROMUK, OracleDatabaseMetaData.grabFKFromUK);
		OracleTable.dumpPhysicalAttributes = Utils.getPropBool(prop, PROP_DUMP_TABLE_PHYSICAL_ATTRIBUTES, OracleTable.dumpPhysicalAttributes);
		OracleTable.dumpLoggingClause = Utils.getPropBool(prop, PROP_DUMP_TABLE_LOGGING, OracleTable.dumpLoggingClause);
	}
	
	/* (non-Javadoc)
	 * @see tbrugz.sqldump.DbmgrFeatures#grabDBObjects(tbrugz.sqldump.SchemaModel, java.lang.String, java.sql.Connection)
	 */
	@Override
	public void grabDBObjects(SchemaModel model, String schemaPattern, Connection conn) throws SQLException {
		if(grabViews) {
			grabDBViews(model, schemaPattern, conn);
			grabDBMaterializedViews(model, schemaPattern, conn);
		}
		if(grabTriggers) {
			grabDBTriggers(model, schemaPattern, conn);
		}
		if(grabExecutables) {
			grabDBExecutables(model, schemaPattern, conn);
		}
		if(grabSynonyms) {
			grabDBSynonyms(model, schemaPattern, conn);
		}
		if(grabIndexes) {
			grabDBIndexes(model, schemaPattern, conn);
		}
		if(grabSequences) {
			grabDBSequences(model, schemaPattern, conn);
		}
		if(grabExtraConstraints) {
			grabDBCheckConstraints(model, schemaPattern, conn);
			grabDBUniqueConstraints(model, schemaPattern, conn);
		}
	}
	
	void grabDBViews(SchemaModel model, String schemaPattern, Connection conn) throws SQLException {
		log.debug("grabbing views");
		String query = "SELECT owner, VIEW_NAME, 'VIEW' as view_type, TEXT FROM ALL_VIEWS "
				+" where owner = '"+schemaPattern+"' ORDER BY VIEW_NAME";
		log.debug("sql: "+query);
		Statement st = conn.createStatement();
		ResultSet rs = st.executeQuery(query);
		
		int count = 0;
		while(rs.next()) {
			View v = new View();
			v.setName( rs.getString(2) );
			v.query = rs.getString(4);
			v.setSchemaName(schemaPattern);
			model.getViews().add(v);
			count++;
		}
		rs.close();
		st.close();
		
		log.info("["+schemaPattern+"]: "+count+" views grabbed");// ["+model.views.size()+"/"+count+"]: ");
	}

	void grabDBMaterializedViews(SchemaModel model, String schemaPattern, Connection conn) throws SQLException {
		log.debug("grabbing materialized views");
		String query = "select owner, mview_name, 'MATERIALIZED_VIEW' AS VIEW_TYPE, query from all_mviews "
				+" where owner = '"+schemaPattern+"' ORDER BY MVIEW_NAME";
		log.debug("sql: "+query);
		Statement st = conn.createStatement();
		ResultSet rs = st.executeQuery(query);
		
		int count = 0;
		while(rs.next()) {
			MaterializedView v = new MaterializedView();
			v.setName( rs.getString(2) );
			v.query = rs.getString(4);
			v.setSchemaName(schemaPattern);
			//v.materialized = true;
			model.getViews().add(v);
			count++;
		}
		rs.close();
		st.close();
		
		log.info("["+schemaPattern+"]: "+count+" materialized views grabbed");
	}
	
	void grabDBTriggers(SchemaModel model, String schemaPattern, Connection conn) throws SQLException {
		log.debug("grabbing triggers");
		String query = "SELECT TRIGGER_NAME, TABLE_OWNER, TABLE_NAME, DESCRIPTION, TRIGGER_BODY "
				+"FROM ALL_TRIGGERS where owner = '"+schemaPattern+"' ORDER BY trigger_name";
		log.debug("sql: "+query);
		Statement st = conn.createStatement();
		ResultSet rs = st.executeQuery(query);
		
		int count = 0;
		while(rs.next()) {
			Trigger t = new Trigger();
			t.setName( rs.getString(1) );
			t.setSchemaName(rs.getString(2));
			t.tableName = rs.getString(3);
			t.description = rs.getString(4);
			t.body = rs.getString(5);
			model.getTriggers().add(t);
			count++;
		}
		rs.close();
		st.close();
		
		log.info("["+schemaPattern+"]: "+count+" triggers grabbed");
	}

	void grabDBExecutables(SchemaModel model, String schemaPattern, Connection conn) throws SQLException {
		log.debug("grabbing executables");
		String query = "select name, type, line, text from all_source "
			+"where type in ('PROCEDURE','PACKAGE','PACKAGE BODY','FUNCTION','TYPE') "
			+"and owner = '"+schemaPattern+"' "
			+"order by type, name, line";
		log.debug("sql: "+query);
		Statement st = conn.createStatement();
		ResultSet rs = st.executeQuery(query);
		
		int count = 0;
		int countExecutables = 0;
		ExecutableObject eo = null;
		StringBuffer sb = null;
		
		while(rs.next()) {
			int line = rs.getInt(3);
			if(line==1) {
				//end last object
				if(eo!=null) {
					eo.setBody( sb.toString() );
					model.getExecutables().add(eo);
					countExecutables++;
				}
				//new object
				eo = new ExecutableObject();
				sb = new StringBuffer();
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
			count++;
		}
		if(sb!=null) {
			eo.setBody( sb.toString() );
			model.getExecutables().add(eo);
			countExecutables++;
		}
		rs.close();
		st.close();
		
		log.info("["+schemaPattern+"]: "+countExecutables+" executable objects grabbed [linecount="+count+"]");
		
		//grabs metadata
		grabDBExecutablesMetadata(model, schemaPattern, conn);
	}
	
	void grabDBExecutablesMetadata(SchemaModel model, String schemaPattern, Connection conn) throws SQLException {
		String query = "select p.owner, p.object_id, p.object_name, p.subprogram_id, p.procedure_name, p.object_type, "
				+"       (select case min(position) when 0 then 'FUNCTION' when 1 then 'PROCEDURE' end from all_arguments aaz where p.object_id = aaz.object_id and p.subprogram_id = aaz.subprogram_id) as subprogram_type, "
				+"       aa.argument_name, aa.position, aa.sequence, aa.data_type, aa.in_out, aa.data_length, aa.data_precision, aa.data_scale, aa.pls_type "
				+"  from all_procedures p "
				+"  left outer join all_arguments aa on p.object_id = aa.object_id and p.subprogram_id = aa.subprogram_id "
				+" where p.owner = '"+schemaPattern+"' "
				//+"   and p.object_type = 'PACKAGE' "
				//+"   and p.procedure_name is not null "
				+"   and aa.position is not null "
				+" order by p.owner, p.object_name, p.subprogram_id, procedure_name, aa.position ";
		log.debug("sql: "+query);
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
			eo = DBIdentifiable.getDBIdentifiableByTypeSchemaAndName(model.getExecutables(), DBObjectType.PROCEDURE, schemaPattern, objectName);
			if(eo==null) { //not a procedure, maybe a function
				eo = DBIdentifiable.getDBIdentifiableByTypeSchemaAndName(model.getExecutables(), DBObjectType.FUNCTION, schemaPattern, objectName);
			}
			//not a top-level procedure or function, maybe declared inside a package
			if(eo==null) {
				eo = DBIdentifiable.getDBIdentifiableByTypeSchemaAndName(model.getExecutables(), DBObjectType.PROCEDURE, schemaPattern, subprogramName);
			}
			if(eo==null) {
				eo = DBIdentifiable.getDBIdentifiableByTypeSchemaAndName(model.getExecutables(), DBObjectType.FUNCTION, schemaPattern, subprogramName);
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
				
				model.getExecutables().add(eo);
				newExecutablesCount++;
			}
			else {
				log.debug("procedure or function found: "+objectName+" / "+subprogramName+" / "+objectType+" / eo="+eo);
			}
			ExecutableParameter ep = new ExecutableParameter();
			ep.name = rs.getString(8);
			ep.dataType = rs.getString(11);
			ep.position = rs.getInt(9);
			String inout = rs.getString(12);
			if(inout!=null) {
				try {
					ep.inout = INOUT.getValue(inout);
				}
				catch(IllegalArgumentException e) {
					log.warn("illegal INOUT value: "+inout);
				}
			}
			//log.info("parameter: "+ep);
			if(ep.position==0) {
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

		log.info("["+schemaPattern+"]: "+newExecutablesCount+" xtra executables grabbed (metadata); "+paramCount+" executable's parameters grabbed");
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
			grant.grantee = grantee;
			grant.privilege = PrivilegeType.EXECUTE;
			grant.table = executable.getName();
			grant.withGrantOption = grantable;
			
			executable.grants.add(grant);
		}
	}

	void grabDBSynonyms(SchemaModel model, String schemaPattern, Connection conn) throws SQLException {
		log.debug("grabbing synonyms");
		String query = "select synonym_name, table_owner, table_name, db_link from all_synonyms "
				+"where owner = '"+schemaPattern+"'";
		log.debug("sql: "+query);
		Statement st = conn.createStatement();
		ResultSet rs = st.executeQuery(query);
		
		int count = 0;
		while(rs.next()) {
			Synonym s = new Synonym();
			s.setName( rs.getString(1) );
			s.setSchemaName(schemaPattern);
			s.objectOwner = rs.getString(2);
			s.referencedObject = rs.getString(3);
			s.dbLink = rs.getString(4);
			model.getSynonyms().add(s);
			count++;
		}
		rs.close();
		st.close();
		
		log.info("["+schemaPattern+"]: "+count+" synonyms grabbed");
	}

	/*
	 * TODO: move to OracleDatabaseMetaData
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
		
		String query = "select ui.table_owner, ui.index_name, ui.uniqueness, ui.index_type, ui.table_name, uic.column_name, uip.partitioning_type, uip.locality "
			+"from all_indexes ui, all_ind_columns uic, all_part_indexes uip "
			+"where UI.INDEX_NAME = UIC.INDEX_NAME "
			+"and ui.table_name = uic.table_name "
			+"and ui.table_owner = uic.table_owner "
			+"and ui.index_name = uip.index_name (+) "
			+"and ui.table_name = uip.table_name (+) "
			+"and ui.table_owner = uip.owner (+) "
			+"and ui.owner = '"+schemaPattern+"' "
			+"order by ui.table_owner, ui.index_name, uic.column_position";
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
					model.getIndexes().add(idx);
					idxCount++;
				}
				//new object
				idx = new Index();
				idx.setName( idxName );
				idx.unique = rs.getString(3).equals("UNIQUE");
				idx.setSchemaName(rs.getString(1));
				setIndexType(idx, rs.getString(4));
				idx.tableName = rs.getString(5);
				if("LOCAL".equals(rs.getString("LOCALITY"))) {
					idx.local = true;
				}
			}
			idx.columns.add(rs.getString(6));
			colCount++;
		}
		if(idx!=null) {
			model.getIndexes().add(idx);
			idxCount++;
		}
		rs.close();
		st.close();
		
		log.info("["+schemaPattern+"]: "+idxCount+" indexes grabbed [colcount="+colCount+"]");
	}
	
	static void setIndexType(Index idx, String typeStr) {
		if(typeStr==null) { return; }
		if(typeStr.equals("NORMAL")) { return; }
		if(typeStr.equals("BITMAP")) { idx.type = typeStr; return; }
		if(typeStr.equals("NORMAL/REV")) { idx.reverse = true; return; }
		//XXX: 'unknown' index types: DOMAIN, FUNCTION-BASED NORMAL, FUNCTION-BASED DOMAIN, IOT - TOP
		idx.comment = "unknown index type: '"+typeStr+"'";
	}

	void grabDBSequences(SchemaModel model, String schemaPattern, Connection conn) throws SQLException {
		log.debug("grabbing sequences");
		String query = "select sequence_name, min_value, increment_by, last_number from all_sequences "
				+"where sequence_owner = '"+schemaPattern+"' order by sequence_name";
		log.debug("sql: "+query);
		Statement st = conn.createStatement();
		ResultSet rs = st.executeQuery(query);
		
		int count = 0;
		while(rs.next()) {
			Sequence s = new Sequence();
			s.setName( rs.getString(1) );
			s.setSchemaName(schemaPattern);
			s.minValue = rs.getLong(2);
			s.incrementBy = rs.getLong(3);
			s.lastNumber = rs.getLong(4);
			model.getSequences().add(s);
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
		if(t instanceof OracleTable) {
			OracleTable ot = (OracleTable) t;
			try {
				ot.tableSpace = rs.getString("TABLESPACE_NAME");
				ot.temporary = "YES".equals(rs.getString("TEMPORARY"));
				ot.logging = "YES".equals(rs.getString("LOGGING"));
				if("YES".equals(rs.getString("PARTITIONED"))) {
					ot.partitioned = true;
					ot.partitionType = OracleTable.PartitionType.valueOf(rs.getString("PARTITIONING_TYPE"));
					
					//get partition type, columns
					getPartitionColumns(ot, rs.getStatement().getConnection());
					//get partitions
					getPartitions(ot, rs.getStatement().getConnection());
				}
			}
			catch(SQLException e) {
				//try {
					//log.warn("OracleSpecific: "+e+"; col names: ("+SQLUtils.getColumnNames(rs.getMetaData())+")");
					log.warn("OracleSpecific: "+e);
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

	void grabDBCheckConstraints(SchemaModel model, String schemaPattern, Connection conn) throws SQLException {
		log.debug("grabbing check constraints");
		
		//check constraints
		String query = "select owner, table_name, constraint_name, constraint_type, search_condition "
				+"from all_constraints "
				+"where owner = '"+schemaPattern+"' "
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
			c.type = Constraint.ConstraintType.CHECK;
			c.setName( rs.getString(3) );
			c.checkDescription = rs.getString(5);
			
			//ignore NOT NULL constraints
			if(c.checkDescription.contains(" IS NOT NULL")) continue;
			
			Table t = (Table) DBObject.findDBObjectBySchemaAndName(model.getTables(), rs.getString(1), tableName);
			if(t!=null) {
				t.getConstraints().add(c);
				count++;
			}
			else {
				countNotAdded++;
				if(c.getName()==null || c.getName().startsWith("BIN$")) {
					log.debug("deleted chack constraint "+c+" can't be added to table '"+tableName+"': table not found");
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

	void grabDBUniqueConstraints(SchemaModel model, String schemaPattern, Connection conn) throws SQLException {
		log.debug("grabbing unique constraints");

		//unique constraints
		String query = "select distinct al.owner, al.table_name, al.constraint_name, column_name, position "
				+"from all_constraints al, all_cons_columns acc "
				+"where al.constraint_name = acc.constraint_name "
				+"and al.owner = '"+schemaPattern+"' "
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
				Table t = (Table) DBObject.findDBObjectBySchemaAndName(model.getTables(), rs.getString(1), tableName);
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
				c.type = Constraint.ConstraintType.UNIQUE;
				c.setName( constraintName );
			}
			c.uniqueColumns.add(rs.getString(4));
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
				log.warn("addFKSpecificFeatures: column 'STATUS', 'VALIDATED' or 'RELY' not avaiable [ex: "+e+"]");
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
	public String sqlAlterColumnClause() {
		return "modify";
	}
}