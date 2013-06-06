package tbrugz.sqldump.dbmsfeatures;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Properties;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import tbrugz.sqldump.dbmodel.Constraint;
import tbrugz.sqldump.dbmodel.DBIdentifiable;
import tbrugz.sqldump.dbmodel.DBObjectType;
import tbrugz.sqldump.dbmodel.ExecutableObject;
import tbrugz.sqldump.dbmodel.ExecutableParameter;
import tbrugz.sqldump.dbmodel.SchemaModel;
import tbrugz.sqldump.dbmodel.Sequence;
import tbrugz.sqldump.dbmodel.Table;
import tbrugz.sqldump.dbmodel.View;
import tbrugz.sqldump.def.DefaultDBMSFeatures;
import tbrugz.sqldump.util.Utils;

public class InformationSchemaFeatures extends DefaultDBMSFeatures {
	private static Log log = LogFactory.getLog(InformationSchemaFeatures.class);

	//boolean dumpSequenceStartWith = true;
	static final Pattern patternLastSemicolon = Pattern.compile(";\\s*$");
	
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
		if(grabCheckConstraints) {
			grabDBCheckConstraints(model, schemaPattern, conn);
		}
		if(grabUniqueConstraints) {
			grabDBUniqueConstraints(model, schemaPattern, conn);
		}
	}
	
	String grabDBViewsQuery(String schemaPattern) {
		return "select table_catalog, table_schema, table_name, view_definition, check_option, is_updatable "
			+"from information_schema.views "
			+"where view_definition is not null "
			+"and table_schema = '"+schemaPattern+"' "
			+"order by table_catalog, table_schema, table_name ";
	}

	void grabDBViews(SchemaModel model, String schemaPattern, Connection conn) throws SQLException {
		String query = grabDBViewsQuery(schemaPattern);
		log.debug("grabbing views: sql:\n"+query);
		Statement st = conn.createStatement();
		ResultSet rs = st.executeQuery(query);
		
		int count = 0;
		while(rs.next()) {
			View v = new View();
			v.setName( rs.getString(3) );
			v.query = rs.getString(4);
			Matcher m = patternLastSemicolon.matcher(v.query);
			if(m.find()) {
				v.query = m.replaceAll("");
			}
			/*if(v.query!=null && v.query.endsWith(";")) {
				v.query = v.query.substring(0, v.query.length()-1);
			}*/
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

	/*
	 * see: http://www.postgresql.org/docs/9.1/static/infoschema-triggers.html
	 */
	String grabDBTriggersQuery(String schemaPattern) {
		return "select trigger_catalog, trigger_schema, trigger_name, event_manipulation, event_object_schema, event_object_table, action_statement, action_orientation, action_timing, action_condition "
			+"from information_schema.triggers "
			+"where trigger_schema = '"+schemaPattern+"' "
			+"order by trigger_catalog, trigger_schema, trigger_name, event_manipulation ";
	}
	
	/*
	 * TODOne: add when_clause - [ WHEN ( condition ) ] - see http://www.postgresql.org/docs/9.1/static/sql-createtrigger.html
	 */
	void grabDBTriggers(SchemaModel model, String schemaPattern, Connection conn) throws SQLException {
		log.debug("grabbing triggers");
		String query = grabDBTriggersQuery(schemaPattern);
		Statement st = conn.createStatement();
		log.debug("sql: "+query);
		ResultSet rs = st.executeQuery(query);
		
		int count = 0;
		int rowcount = 0;
		while(rs.next()) {
			String schemaName = rs.getString(2);
			String name = rs.getString(3);
			InformationSchemaTrigger t = DBIdentifiable.getDBIdentifiableBySchemaAndName(model.getTriggers(), schemaName, name);
			
			if(t==null) {
				t = new InformationSchemaTrigger();
				t.setSchemaName( schemaName );
				t.setName( name );
				model.getTriggers().add(t);
				count++;
			}
			t.eventsManipulation.add(rs.getString(4));
			t.tableName = rs.getString(6);
			t.actionStatement = rs.getString(7);
			t.actionOrientation = rs.getString(8);
			t.conditionTiming = rs.getString(9);
			t.whenClause = rs.getString(10);
			rowcount++;
		}
		
		rs.close();
		st.close();
		log.info(count+" triggers grabbed [rowcount="+rowcount+"]");
	}

	String grabDBRoutinesQuery(String schemaPattern) {
		return "select routine_name, routine_type, r.data_type, external_language, routine_definition, p.parameter_name, p.data_type, p.ordinal_position "
				+"from information_schema.routines r, information_schema.parameters p "
				+"where r.specific_name = p.specific_name and r.routine_definition is not null "
				+"and r.specific_schema = '"+schemaPattern+"' "
				+"order by routine_catalog, routine_schema, routine_name, p.ordinal_position ";
	}
	
	void grabDBRoutines(SchemaModel model, String schemaPattern, Connection conn) throws SQLException {
		log.debug("grabbing executables");
		String query = grabDBRoutinesQuery(schemaPattern);
		log.debug("sql: "+query);
		Statement st = conn.createStatement();
		ResultSet rs = st.executeQuery(query);
		
		int count = 0;
		InformationSchemaRoutine eo = null;
		while(rs.next()) {

			String routineName = rs.getString(1);
			if(eo==null || !routineName.equals(eo.getName())) {
				//end last object
				if(eo!=null) {
					if(addExecutableToModel(model, eo)) {
						count++;
					}
				}

				//new object
				eo = new InformationSchemaRoutine();
				eo.setSchemaName( schemaPattern );
				eo.setName( routineName );
				try {
					eo.setType( DBObjectType.valueOf(Utils.normalizeEnumStringConstant(rs.getString(2))) );
				}
				catch(IllegalArgumentException iae) {
					log.warn("unknown object type: "+rs.getString(2));
					eo.setType( DBObjectType.EXECUTABLE );
				}
				ExecutableParameter ep = new ExecutableParameter();
				ep.dataType = rs.getString(3);
				eo.setReturnParam(ep);
				eo.setParams(new ArrayList<ExecutableParameter>());
				
				eo.externalLanguage = rs.getString(4);
				eo.setBody( rs.getString(5) );
			}
			ExecutableParameter ep = new ExecutableParameter();
			ep.name = rs.getString(6);
			ep.dataType = rs.getString(7);
			ep.position = rs.getInt(8);
			eo.getParams().add(ep);
		}
		if(eo!=null) {
			if(addExecutableToModel(model, eo)) {
				count++;
			}
		}
		
		rs.close();
		st.close();
		log.info(count+" executable objects/routines grabbed");
	}
	
	boolean addExecutableToModel(SchemaModel model, ExecutableObject eo) {
		boolean added = model.getExecutables().add(eo);
		if(!added) {
			boolean b1 = model.getExecutables().remove(eo);
			boolean b2 = model.getExecutables().add(eo);
			added = b1 && b2;
			if(added) {
				log.debug("executable ["+eo.getType()+"] '"+eo.getQualifiedName()+"' replaced in model");
			}
			else {
				log.warn("executable ["+eo.getType()+"] '"+eo.getQualifiedName()+"' not added to model [removed: "+b1+" ; added: "+b2+"]");
			}
			//log.warn("executable could not be added to model: "+eo.toString()+" (already added?)");
		}
		return added;
	}

	String grabDBSequencesQuery(String schemaPattern) {
		return "select sequence_name, minimum_value, increment, maximum_value " // increment_by, last_number?
				+"from information_schema.sequences "
				+"where sequence_schema = '"+schemaPattern+"' "
				+"order by sequence_catalog, sequence_schema, sequence_name ";
	}
	
	void grabDBSequences(SchemaModel model, String schemaPattern, Connection conn) throws SQLException {
		Statement st = conn.createStatement();
		String query = grabDBSequencesQuery(schemaPattern);
		log.debug("grabbing sequences: sql:\n"+query);
		ResultSet rs = st.executeQuery(query);
		
		int count = 0;
		while(rs.next()) {
			Sequence s = new Sequence();
			s.setSchemaName( schemaPattern );
			s.setName( rs.getString(1) );
			String minvalueStr = rs.getString(2);
			if(minvalueStr!=null) {
				s.minValue = rs.getLong(2);
			}
			s.incrementBy = 1; //rs.getLong(3);
			//s.lastNumber = rs.getLong(4);
			model.getSequences().add(s);
			count++;
		}
		
		rs.close();
		st.close();
		log.info(count+" sequences grabbed");
	}

	String grabDBCheckConstraintsQuery(String schemaPattern) {
		return "select cc.constraint_schema, table_name, cc.constraint_name, check_clause " 
				+"from information_schema.check_constraints cc, information_schema.constraint_column_usage ccu "
				+"where cc.constraint_name = ccu.constraint_name "
				+"and cc.constraint_schema = '"+schemaPattern+"' "
				+"order by table_name, constraint_name ";
	}
	
	void grabDBCheckConstraints(SchemaModel model, String schemaPattern, Connection conn) throws SQLException {
		log.debug("grabbing check constraints");
		
		String query = grabDBCheckConstraintsQuery(schemaPattern);
		Statement st = conn.createStatement();
		log.debug("sql: "+query);
		ResultSet rs = st.executeQuery(query);
		
		int count = 0;
		int countConstraints = 0;
		while(rs.next()) {
			String schemaName = rs.getString(1);
			String tableName = rs.getString(2);
			
			Constraint c = new Constraint();
			c.type = Constraint.ConstraintType.CHECK;
			c.setName( rs.getString(3) );
			c.checkDescription = rs.getString(4);
			Table t = DBIdentifiable.getDBIdentifiableBySchemaAndName(model.getTables(), schemaName, tableName);
			if(t!=null) {
				t.getConstraints().add(c);
				countConstraints++;
			}
			else {
				log.warn("constraint "+c+" can't be added to table '"+tableName+"': table not found");
			}
			count++;
		}
		
		rs.close();
		st.close();
		log.info(countConstraints+" check constraints grabbed [rowcount="+count+"]");
	}

	//order by "column_position"? see grabDBUniqueConstraints()
	String grabDBUniqueConstraintsQuery(String schemaPattern) {
		return "select tc.constraint_schema, tc.table_name, tc.constraint_name, column_name " 
				+"from information_schema.table_constraints tc, information_schema.constraint_column_usage ccu "
				+"where tc.constraint_name = ccu.constraint_name "
				+"and tc.constraint_schema = '"+schemaPattern+"' "
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
				Table t = DBIdentifiable.getDBIdentifiableBySchemaAndName(model.getTables(), schemaName, tableName);
				if(t!=null) {
					t.getConstraints().add(c);
					countUniqueConstraints++;
				}
				else {
					log.warn("constraint "+c+" can't be added to table '"+tableName+"': table not found");
				}
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
