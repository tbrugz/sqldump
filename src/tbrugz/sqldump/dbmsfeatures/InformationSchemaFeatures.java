package tbrugz.sqldump.dbmsfeatures;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import tbrugz.sqldump.dbmodel.Constraint;
import tbrugz.sqldump.dbmodel.DBObject;
import tbrugz.sqldump.dbmodel.DBObjectType;
import tbrugz.sqldump.dbmodel.SchemaModel;
import tbrugz.sqldump.dbmodel.Sequence;
import tbrugz.sqldump.dbmodel.Table;
import tbrugz.sqldump.dbmodel.View;
import tbrugz.sqldump.def.DefaultDBMSFeatures;
import tbrugz.sqldump.util.Utils;

public class InformationSchemaFeatures extends DefaultDBMSFeatures {
	static Log log = LogFactory.getLog(InformationSchemaFeatures.class);

	//boolean dumpSequenceStartWith = true;
	
	public void procProperties(Properties prop) {
		super.procProperties(prop);
		//Sequence.dumpStartWith = "true".equals(prop.getProperty(PROP_SEQUENCE_STARTWITHDUMP));
	}
	
	public void grabDBObjects(SchemaModel model, String schemaPattern, Connection conn) throws SQLException {
		if(grabViews) {
			grabDBViews(model, schemaPattern, conn);
		}
		if(grabTriggers) {
			grabDBTriggers(model, schemaPattern, conn);
		}
		if(grabExecutables) {
			grabDBRoutines(model, schemaPattern, conn);
		}
		//if(grabIndexes) {
			//grabDBIndexes(model, schemaPattern, conn);
		//}
		if(grabSequences) {
			grabDBSequences(model, schemaPattern, conn);
		}
		if(grabExtraConstraints) {
			grabDBCheckConstraints(model, schemaPattern, conn);
			grabDBUniqueConstraints(model, schemaPattern, conn);
		}
	}
	
	String grabDBViewsQuery(String schemaPattern) {
		return "select table_catalog, table_schema, table_name, view_definition, check_option, is_updatable "
			+"from information_schema.views "
			+"where view_definition is not null "
			+"order by table_catalog, table_schema, table_name ";
	}

	void grabDBViews(SchemaModel model, String schemaPattern, Connection conn) throws SQLException {
		log.debug("grabbing views");
		String query = grabDBViewsQuery(schemaPattern);
		Statement st = conn.createStatement();
		ResultSet rs = st.executeQuery(query);
		
		int count = 0;
		while(rs.next()) {
			View v = new View();
			v.name = rs.getString(3);
			v.query = rs.getString(4);
			v.query = v.query.substring(0, v.query.length()-2);
			v.setSchemaName( schemaPattern );
			v.checkOption = View.CheckOptionType.valueOf(rs.getString(5)); //"YES".equalsIgnoreCase(rs.getString(5));
			v.withReadOnly = !"YES".equalsIgnoreCase(rs.getString(6));
			model.getViews().add(v);
			count++;
		}
		
		rs.close();
		st.close();
		log.info(count+" views grabbed");
	}

	String grabDBTriggersQuery(String schemaPattern) {
		return "select trigger_catalog, trigger_schema, trigger_name, event_manipulation, event_object_schema, event_object_table, action_statement, action_orientation, action_timing "
			+"from information_schema.triggers "
			+"order by trigger_catalog, trigger_schema, trigger_name, event_manipulation ";
	}
	
	void grabDBTriggers(SchemaModel model, String schemaPattern, Connection conn) throws SQLException {
		log.debug("grabbing triggers");
		String query = grabDBTriggersQuery(schemaPattern);
		Statement st = conn.createStatement();
		log.debug("sql: "+query);
		ResultSet rs = st.executeQuery(query);
		
		int count = 0;
		while(rs.next()) {
			String schemaName = rs.getString(2);
			String name = rs.getString(3);
			InformationSchemaTrigger t = (InformationSchemaTrigger) DBObject.findDBObjectBySchemaAndName(model.getTriggers(), schemaName, name);
			
			if(t==null) {
				t = new InformationSchemaTrigger();
				model.getTriggers().add(t);
				count++;
			}
			t.setSchemaName( schemaName );
			t.name = name;
			t.eventsManipulation.add(rs.getString(4));
			t.tableName = rs.getString(6);
			t.actionStatement = rs.getString(7);
			t.actionOrientation = rs.getString(8);
			t.conditionTiming = rs.getString(9);
		}
		
		rs.close();
		st.close();
		log.info(count+" triggers grabbed");
	}

	String grabDBRoutinesQuery(String schemaPattern) {
		return "select routine_name, routine_type, data_type, external_language, routine_definition "
				+"from information_schema.routines "
				+"where routine_definition is not null "
				+"order by routine_catalog, routine_schema, routine_name ";
	}
	
	void grabDBRoutines(SchemaModel model, String schemaPattern, Connection conn) throws SQLException {
		log.debug("grabbing executables");
		String query = grabDBRoutinesQuery(schemaPattern);
		Statement st = conn.createStatement();
		ResultSet rs = st.executeQuery(query);
		
		int count = 0;
		while(rs.next()) {
			InformationSchemaRoutine eo = new InformationSchemaRoutine();
			eo.setSchemaName( schemaPattern );
			eo.name = rs.getString(1);
			try {
				eo.type = DBObjectType.valueOf(Utils.normalizeEnumStringConstant(rs.getString(2)));
			}
			catch(IllegalArgumentException iae) {
				log.warn("unknown object type: "+rs.getString(2));
				eo.type = DBObjectType.EXECUTABLE;
			}
			eo.returnType = rs.getString(3);
			eo.externalLanguage = rs.getString(4);
			eo.body = rs.getString(5);
			model.getExecutables().add(eo);
			count++;
		}
		
		rs.close();
		st.close();
		log.info(count+" executable objects/routines grabbed");
	}

	void grabDBSequences(SchemaModel model, String schemaPattern, Connection conn) throws SQLException {
		log.debug("grabbing sequences");
		String query = "select sequence_name, minimum_value, increment, maximum_value " // increment_by, last_number?
				+"from information_schema.sequences "
				+"order by sequence_catalog, sequence_schema, sequence_name ";
		Statement st = conn.createStatement();
		ResultSet rs = st.executeQuery(query);
		
		int count = 0;
		while(rs.next()) {
			Sequence s = new Sequence();
			s.setSchemaName( schemaPattern );
			s.name = rs.getString(1);
			s.minValue = rs.getLong(2);
			s.incrementBy = 1; //rs.getLong(3);
			//s.lastNumber = rs.getLong(4);
			model.getSequences().add(s);
			count++;
		}
		
		rs.close();
		st.close();
		log.info(count+" sequences grabbed");
	}

	void grabDBCheckConstraints(SchemaModel model, String schemaPattern, Connection conn) throws SQLException {
		log.debug("grabbing check constraints");
		
		String query = "select cc.constraint_schema, table_name, cc.constraint_name, check_clause " 
				+"from information_schema.check_constraints cc, information_schema.constraint_column_usage ccu "
				+"where cc.constraint_name = ccu.constraint_name "
				+"order by table_name, constraint_name ";
		Statement st = conn.createStatement();
		log.debug("sql: "+query);
		ResultSet rs = st.executeQuery(query);
		
		int count = 0;
		while(rs.next()) {
			String schemaName = rs.getString(1);
			String tableName = rs.getString(2);
			
			Constraint c = new Constraint();
			c.type = Constraint.ConstraintType.CHECK;
			c.setName( rs.getString(3) );
			c.checkDescription = rs.getString(4);
			Table t = (Table) DBObject.findDBObjectBySchemaAndName(model.getTables(), schemaName, tableName);
			if(t!=null) {
				t.getConstraints().add(c);
			}
			else {
				log.warn("constraint "+c+" can't be added to table '"+tableName+"': table not found");
			}
			count++;
		}
		
		rs.close();
		st.close();
		log.info(count+" check constraints grabbed");
	}

	String grabDBUniqueConstraintsQuery(String schemaPattern) {
		return "select tc.constraint_schema, tc.table_name, tc.constraint_name, column_name " 
				+"from information_schema.table_constraints tc, information_schema.constraint_column_usage ccu "
				+"where tc.constraint_name = ccu.constraint_name "
				+"and constraint_type = 'UNIQUE' "
				+"order by table_name, constraint_name, column_name ";
	}
	
	void grabDBUniqueConstraints(SchemaModel model, String schemaPattern, Connection conn) throws SQLException {
		log.debug("grabbing unique constraints");

		//XXX: table constraint_column_usage has no 'column_order' column... ordering by column name
		String query = grabDBUniqueConstraintsQuery(schemaPattern);
		Statement st = conn.createStatement();
		log.debug("sql: "+query);
		ResultSet rs = st.executeQuery(query);
		
		int count = 0;
		int countUniqueConstraints = 0;
		String previousConstraintId = null;
		Constraint c = null;
		while(rs.next()) {
			String schemaName = rs.getString(1);
			String tableName = rs.getString(2);
			String constraintName = rs.getString(3);
			String constraintId = tableName+"."+constraintName;
			
			if(!constraintId.equals(previousConstraintId)) {
				c = new Constraint();
				c.type = Constraint.ConstraintType.UNIQUE;
				c.setName( constraintName );
				Table t = (Table) DBObject.findDBObjectBySchemaAndName(model.getTables(), schemaName, tableName);
				if(t!=null) {
					t.getConstraints().add(c);
				}
				else {
					log.warn("constraint "+c+" can't be added to table '"+tableName+"': table not found");
				}
				countUniqueConstraints++;
			}
			c.uniqueColumns.add(rs.getString(4));
			previousConstraintId = constraintId;
			count++;
		}
		
		rs.close();
		st.close();
		log.info(countUniqueConstraints+" unique constraints grabbed [colcount="+count+"]");
	}
	
}
