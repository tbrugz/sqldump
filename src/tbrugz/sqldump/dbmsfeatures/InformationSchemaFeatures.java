package tbrugz.sqldump.dbmsfeatures;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;

import org.apache.log4j.Logger;

import tbrugz.sqldump.DefaultDBMSFeatures;
import tbrugz.sqldump.SchemaModel;
import tbrugz.sqldump.Utils;
import tbrugz.sqldump.dbmodel.Constraint;
import tbrugz.sqldump.dbmodel.DBObject;
import tbrugz.sqldump.dbmodel.DBObjectType;
import tbrugz.sqldump.dbmodel.Sequence;
import tbrugz.sqldump.dbmodel.Table;
import tbrugz.sqldump.dbmodel.View;

public class InformationSchemaFeatures extends DefaultDBMSFeatures {
	static Logger log = Logger.getLogger(InformationSchemaFeatures.class);

	//boolean grabIndexes = false;
	//boolean dumpSequenceStartWith = true;
	
	public void procProperties(Properties prop) {
		//grabIndexes = "true".equals(prop.getProperty(PROP_GRAB_INDEXES));
		//Sequence.dumpStartWith = "true".equals(prop.getProperty(PROP_SEQUENCE_STARTWITHDUMP));
	}
	
	public void grabDBObjects(SchemaModel model, String schemaPattern, Connection conn) throws SQLException {
		grabDBViews(model, schemaPattern, conn);
		grabDBTriggers(model, schemaPattern, conn);
		grabDBRoutines(model, schemaPattern, conn);
		//if(grabIndexes) {
			//grabDBIndexes(model, schemaPattern, conn);
		//}
		grabDBSequences(model, schemaPattern, conn);
		grabDBConstraints(model, schemaPattern, conn);
	}
	
	void grabDBViews(SchemaModel model, String schemaPattern, Connection conn) throws SQLException {
		log.debug("grabbing views");
		String query = "select table_catalog, table_schema, table_name, view_definition "
				+"from information_schema.views "
				+"where view_definition is not null "
				+"order by table_catalog, table_schema, table_name ";
		Statement st = conn.createStatement();
		ResultSet rs = st.executeQuery(query);
		
		int count = 0;
		while(rs.next()) {
			View v = new View();
			v.name = rs.getString(3);
			v.query = rs.getString(4);
			v.query = v.query.substring(0, v.query.length()-2);
			v.schemaName = schemaPattern;
			model.getViews().add(v);
			count++;
		}
		
		log.info(count+" views grabbed");// ["+model.views.size()+"/"+count+"]: ");
	}

	void grabDBTriggers(SchemaModel model, String schemaPattern, Connection conn) throws SQLException {
		log.debug("grabbing triggers");
		String query = "select trigger_catalog, trigger_schema, trigger_name, event_manipulation, event_object_schema, event_object_table, action_statement, action_orientation, condition_timing "
				+"from information_schema.triggers "
				+"order by trigger_catalog, trigger_schema, trigger_name, event_manipulation ";
		Statement st = conn.createStatement();
		ResultSet rs = st.executeQuery(query);
		
		int count = 0;
		while(rs.next()) {
			//InformationSchemaTrigger t = (InformationSchemaTrigger) findTrigger(model.getTriggers(), rs.getString(3));
			String schemaName = rs.getString(2);
			String name = rs.getString(3);
			InformationSchemaTrigger t = (InformationSchemaTrigger) DBObject.findDBObjectBySchemaAndName(model.getTriggers(), schemaName, name);
			
			if(t==null) {
				t = new InformationSchemaTrigger();
				model.getTriggers().add(t);
				count++;
			}
			t.schemaName = schemaName;
			t.name = name;
			t.eventsManipulation.add(rs.getString(4));
			t.tableName = rs.getString(6);
			//t.description = rs.getString(4);
			//t.body = rs.getString(7);
			t.actionStatement = rs.getString(7);
			t.actionOrientation = rs.getString(8);
			t.conditionTiming = rs.getString(9);
		}
		
		log.info(count+" triggers grabbed");
	}
	
	/*static Trigger findTrigger(Set<Trigger> triggers, String name) {
		for(Trigger t: triggers) {
			if(t.name.equals(name)) return t;
		}
		return null;
	}*/

	void grabDBRoutines(SchemaModel model, String schemaPattern, Connection conn) throws SQLException {
		log.debug("grabbing executables");
		String query = "select routine_name, routine_type, data_type, external_language, routine_definition "
				+"from information_schema.routines "
				+"where routine_definition is not null "
				+"order by routine_catalog, routine_schema, routine_name ";
		Statement st = conn.createStatement();
		ResultSet rs = st.executeQuery(query);
		
		int count = 0;
		while(rs.next()) {
			InformationSchemaRoutine eo = new InformationSchemaRoutine();
			eo.schemaName = schemaPattern;
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
			s.schemaName = schemaPattern;
			s.name = rs.getString(1);
			s.minValue = rs.getLong(2);
			s.incrementBy = 1; //rs.getLong(3);
			//s.lastNumber = rs.getLong(4);
			model.getSequences().add(s);
			count++;
		}
		
		log.info(count+" sequences grabbed");
	}

	void grabDBConstraints(SchemaModel model, String schemaPattern, Connection conn) throws SQLException {
		log.debug("grabbing constraints");
		
		//check constraints
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
		
		log.info(count+" check constraints grabbed");

		//unique constraints
		query = "select tc.constraint_schema, tc.table_name, tc.constraint_name, column_name " 
				+"from information_schema.table_constraints tc, information_schema.constraint_column_usage ccu "
				+"where tc.constraint_name = ccu.constraint_name "
				+"and constraint_type = 'UNIQUE' "
				+"order by table_name, constraint_name, column_name ";
		st = conn.createStatement();
		log.debug("sql: "+query);
		rs = st.executeQuery(query);
		
		count = 0;
		int countUniqueConstraints = 0;
		String previousConstraint = null;
		Constraint c = null;
		while(rs.next()) {
			String schemaName = rs.getString(1);
			String tableName = rs.getString(2);
			String constraintName = rs.getString(3);
			
			if(!constraintName.equals(previousConstraint)) {
				c = new Constraint();
				Table t = (Table) DBObject.findDBObjectBySchemaAndName(model.getTables(), schemaName, tableName);
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
