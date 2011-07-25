package tbrugz.sqldump.dbmsfeatures;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;

import org.apache.log4j.Logger;

import tbrugz.sqldump.AbstractDBMSFeatures;
import tbrugz.sqldump.SQLUtils;
import tbrugz.sqldump.SchemaModel;
import tbrugz.sqldump.Utils;
import tbrugz.sqldump.dbmodel.Column;
import tbrugz.sqldump.dbmodel.Constraint;
import tbrugz.sqldump.dbmodel.DBObject;
import tbrugz.sqldump.dbmodel.DBObjectType;
import tbrugz.sqldump.dbmodel.ExecutableObject;
import tbrugz.sqldump.dbmodel.Index;
import tbrugz.sqldump.dbmodel.Sequence;
import tbrugz.sqldump.dbmodel.Synonym;
import tbrugz.sqldump.dbmodel.Table;
import tbrugz.sqldump.dbmodel.Trigger;
import tbrugz.sqldump.dbmodel.View;

public class OracleFeatures extends AbstractDBMSFeatures {
	static Logger log = Logger.getLogger(OracleFeatures.class);

	boolean grabIndexes = false;
	boolean dumpSequenceStartWith = true;
	
	public void procProperties(Properties prop) {
		//String grabIndexesStr = prop.getProperty(PROP_GRAB_INDEXES);
		//grabIndexes = "true".equals(grabIndexesStr);
		grabIndexes = "true".equals(prop.getProperty(PROP_GRAB_INDEXES));
		//dumpSequenceStartWith = "true".equals(prop.getProperty(PROP_SEQUENCE_STARTWITHDUMP));
		Sequence.dumpStartWith = "true".equals(prop.getProperty(PROP_SEQUENCE_STARTWITHDUMP));
	}
	
	/* (non-Javadoc)
	 * @see tbrugz.sqldump.DbmgrFeatures#grabDBObjects(tbrugz.sqldump.SchemaModel, java.lang.String, java.sql.Connection)
	 */
	public void grabDBObjects(SchemaModel model, String schemaPattern, Connection conn) throws SQLException {
		grabDBViews(model, schemaPattern, conn);
		grabDBTriggers(model, schemaPattern, conn);
		grabDBExecutables(model, schemaPattern, conn);
		grabDBSynonyms(model, schemaPattern, conn);
		if(grabIndexes) {
			grabDBIndexes(model, schemaPattern, conn);
		}
		grabDBSequences(model, schemaPattern, conn);
		grabDBConstraints(model, schemaPattern, conn);
	}
	
	void grabDBViews(SchemaModel model, String schemaPattern, Connection conn) throws SQLException {
		log.debug("grabbing views");
		String query = "SELECT VIEW_NAME, TEXT FROM USER_VIEWS ORDER BY VIEW_NAME";
		Statement st = conn.createStatement();
		ResultSet rs = st.executeQuery(query);
		
		int count = 0;
		while(rs.next()) {
			View v = new View();
			v.name = rs.getString(1);
			v.query = rs.getString(2);
			v.schemaName = schemaPattern;
			model.getViews().add(v);
			count++;
		}
		
		log.info(count+" views grabbed");// ["+model.views.size()+"/"+count+"]: ");
	}

	void grabDBTriggers(SchemaModel model, String schemaPattern, Connection conn) throws SQLException {
		log.debug("grabbing triggers");
		String query = "SELECT TRIGGER_NAME, TABLE_OWNER, TABLE_NAME, DESCRIPTION, TRIGGER_BODY FROM USER_TRIGGERS";
		Statement st = conn.createStatement();
		ResultSet rs = st.executeQuery(query);
		
		int count = 0;
		while(rs.next()) {
			Trigger t = new Trigger();
			t.name = rs.getString(1);
			t.schemaName = rs.getString(2);
			t.tableName = rs.getString(3);
			t.description = rs.getString(4);
			t.body = rs.getString(5);
			model.getTriggers().add(t);
			count++;
		}
		
		log.info(count+" triggers grabbed");
	}

	void grabDBExecutables(SchemaModel model, String schemaPattern, Connection conn) throws SQLException {
		log.debug("grabbing executables");
		String query = "select name, type, line, text from user_source "
			+"where type in ('PROCEDURE','PACKAGE','PACKAGE BODY','FUNCTION','TYPE') "
			+"order by type, name, line";
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
					eo.body = sb.toString();
					model.getExecutables().add(eo);
					countExecutables++;
				}
				//new object
				eo = new ExecutableObject();
				sb = new StringBuffer();
				eo.name = rs.getString(1);
				try {
					eo.type = DBObjectType.valueOf(Utils.normalizeEnumStringConstant(rs.getString(2)));
				}
				catch(IllegalArgumentException iae) {
					log.warn("unknown object type: "+rs.getString(2));
					eo.type = DBObjectType.EXECUTABLE;
				}
				
				eo.schemaName = schemaPattern;
			}
			sb.append(rs.getString(4)); //+"\n"
			count++;
		}
		if(sb!=null) {
			eo.body = sb.toString();
			model.getExecutables().add(eo);
			countExecutables++;
		}
		
		log.info(countExecutables+" executable objects grabbed [linecount="+count+"]");
	}

	void grabDBSynonyms(SchemaModel model, String schemaPattern, Connection conn) throws SQLException {
		log.debug("grabbing synonyms");
		String query = "select synonym_name, table_owner, table_name, db_link from user_synonyms";
		Statement st = conn.createStatement();
		ResultSet rs = st.executeQuery(query);
		
		int count = 0;
		while(rs.next()) {
			Synonym s = new Synonym();
			s.name = rs.getString(1);
			s.schemaName = schemaPattern;
			s.objectOwner = rs.getString(2);
			s.referencedObject = rs.getString(3);
			s.dbLink = rs.getString(4);
			model.getSynonyms().add(s);
			count++;
		}
		
		log.info(count+" synonyms grabbed");
	}

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
			
		String query = "select ui.table_owner, ui.index_name, ui.uniqueness, ui.table_name, uic.column_name "
			+"from user_indexes ui, user_ind_columns uic "
			+"where UI.INDEX_NAME = UIC.INDEX_NAME "
			+"order by ui.table_owner, ui.index_name, uic.column_position";
		Statement st = conn.createStatement();
		ResultSet rs = st.executeQuery(query);
		
		int count = 0;
		Index idx = null;
		
		while(rs.next()) {
			String idxName = rs.getString(2);
			//if first (idx==null) or new index
			if(idx==null || !idxName.equals(idx.name)) {
				//end last object
				if(idx!=null) {
					model.getIndexes().add(idx);
				}
				//new object
				idx = new Index();
				idx.name = idxName;
				idx.unique = rs.getString(3).equals("UNIQUE");
				idx.schemaName = rs.getString(1);
				idx.tableName = rs.getString(4);
			}
			idx.columns.add(rs.getString(5));
			count++;
		}
		model.getIndexes().add(idx);
		
		log.info(model.getIndexes().size()+" indexes grabbed [colcount="+count+"]");
	}

	void grabDBSequences(SchemaModel model, String schemaPattern, Connection conn) throws SQLException {
		log.debug("grabbing sequences");
		String query = "select sequence_name, min_value, increment_by, last_number from user_sequences order by sequence_name";
		Statement st = conn.createStatement();
		ResultSet rs = st.executeQuery(query);
		
		int count = 0;
		while(rs.next()) {
			Sequence s = new Sequence();
			s.name = rs.getString(1);
			s.schemaName = schemaPattern;
			s.minValue = rs.getLong(2);
			s.incrementBy = rs.getLong(3);
			s.lastNumber = rs.getLong(4);
			model.getSequences().add(s);
			count++;
		}
		
		log.info(count+" sequences grabbed");
	}
	
	public DatabaseMetaData getMetadataDecorator(DatabaseMetaData metadata) {
		return new OracleDatabaseMetaData(metadata);
	}
	
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
			}
			catch(SQLException e) {
				try {
					log.warn("OracleSpecific: "+e+"; col names: ("+SQLUtils.getColumnNames(rs.getMetaData())+")");
				} catch (SQLException e1) {
					e1.printStackTrace();
				}
				//e.printStackTrace();
			}
		}
		else {
			log.warn("Table "+t+" should be instance of OracleTable");
		}
	}
	
	@Override
	public void addColumnSpecificFeatures(Column c, ResultSet rs) {
		String dataDefault;
		try {
			dataDefault = rs.getString("DATA_DEFAULT");
			if(dataDefault!=null) {
				c.setDefaultValue(dataDefault.trim());
			}
		} catch (SQLException e) {
			log.warn("resultset has no 'DATA_DEFAULT' column [c: "+c+"]");
		}
	}

	void grabDBConstraints(SchemaModel model, String schemaPattern, Connection conn) throws SQLException {
		log.debug("grabbing constraints");
		
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
			}
			else {
				log.warn("constraint "+c+" can't be added to table '"+tableName+"': table not found");
			}
			count++;
		}
		
		log.info(count+" check constraints grabbed");

		//unique constraints
		query = "select distinct al.owner, al.table_name, al.constraint_name, column_name, position "
				+"from all_constraints al, all_cons_columns acc "
				+"where al.constraint_name = acc.constraint_name "
				+"and al.owner = '"+schemaPattern+"' "
				+"and constraint_type = 'U' "
				+"order by owner, table_name, constraint_name, position, column_name ";
		st = conn.createStatement();
		log.debug("sql: "+query);
		rs = st.executeQuery(query);
		
		count = 0;
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
				}
				else {
					log.warn("constraint "+c+" can't be added to table '"+tableName+"': table not found");
				}
				c.type = Constraint.ConstraintType.UNIQUE;
				c.setName( constraintName );
				countUniqueConstraints++;
			}
			c.uniqueColumns.add(rs.getString(4));
			previousConstraint = constraintName;
			count++;
		}
		
		log.info(countUniqueConstraints+" unique constraints grabbed [colcount="+count+"]");
		
	}
	
}
