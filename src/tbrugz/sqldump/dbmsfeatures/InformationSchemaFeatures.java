package tbrugz.sqldump.dbmsfeatures;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Properties;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import tbrugz.sqldump.dbmd.DefaultDBMSFeatures;
import tbrugz.sqldump.dbmodel.Constraint;
import tbrugz.sqldump.dbmodel.DBIdentifiable;
import tbrugz.sqldump.dbmodel.DBObjectType;
import tbrugz.sqldump.dbmodel.ExecutableObject;
import tbrugz.sqldump.dbmodel.ExecutableParameter;
import tbrugz.sqldump.dbmodel.SchemaModel;
import tbrugz.sqldump.dbmodel.Sequence;
import tbrugz.sqldump.dbmodel.Table;
import tbrugz.sqldump.dbmodel.Trigger;
import tbrugz.sqldump.dbmodel.View;

/*
 * see: https://en.wikipedia.org/wiki/Information_schema
 * FIXME: add object names filters
 * TODO: use bind parameter in SQL queries
 */
public class InformationSchemaFeatures extends DefaultDBMSFeatures {
	private static final Log log = LogFactory.getLog(InformationSchemaFeatures.class);

	//boolean dumpSequenceStartWith = true;
	static final Pattern patternLastSemicolon = Pattern.compile(";\\s*$");
	
	public static final String DEFAULT_SCHEMA = "information_schema";
	
	String informationSchema = DEFAULT_SCHEMA;
	
	static final DBObjectType[] execTypes = {
		DBObjectType.FUNCTION, DBObjectType.PROCEDURE
	};
	
	static final DBObjectType[] supportedTypes = {
		DBObjectType.TABLE, DBObjectType.FK, DBObjectType.VIEW, DBObjectType.INDEX, DBObjectType.EXECUTABLE,
		DBObjectType.TRIGGER, DBObjectType.SEQUENCE,
		//DBObjectType.SYNONYM, DBObjectType.GRANT, DBObjectType.MATERIALIZED_VIEW
	};
	
	@Override
	public void procProperties(Properties prop) {
		super.procProperties(prop);
		//Sequence.dumpStartWith = "true".equals(prop.getProperty(PROP_SEQUENCE_STARTWITHDUMP));
	}
	
	@Override
	public void grabDBObjects(SchemaModel model, String schemaPattern, Connection conn) throws SQLException {
		if(grabViews) {
			grabDBViews(model.getViews(), schemaPattern, null, conn);
		}
		if(grabTriggers) {
			grabDBTriggers(model.getTriggers(), schemaPattern, null, null, conn);
		}
		if(grabExecutables) {
			grabDBExecutables(model.getExecutables(), schemaPattern, null, conn);
		}
		//if(grabIndexes) {
			//grabDBIndexes(model, schemaPattern, conn);
		//}
		if(grabSequences) {
			grabDBSequences(model.getSequences(), schemaPattern, null, conn);
		}
		if(grabCheckConstraints) {
			grabDBCheckConstraints(model.getTables(), schemaPattern, null, null, conn);
		}
		if(grabUniqueConstraints) {
			grabDBUniqueConstraints(model.getTables(), schemaPattern, null, null, conn);
		}
	}
	
	String grabDBViewsQuery(String schemaPattern, String viewNamePattern) {
		return "select table_catalog, table_schema, table_name, view_definition, check_option, is_updatable "
			+"\nfrom "+informationSchema+".views "
			+"\nwhere view_definition is not null "
			+"and table_schema = '"+schemaPattern+"' "
			+(viewNamePattern!=null?"and table_name = '"+viewNamePattern+"' ":"")
			+"order by table_catalog, table_schema, table_name ";
	}

	@Override
	public void grabDBViews(Collection<View> views, String schemaPattern, String viewNamePattern, Connection conn) throws SQLException {
		String query = grabDBViewsQuery(schemaPattern, viewNamePattern);
		log.debug("grabbing views: sql:\n"+query);
		Statement st = conn.createStatement();
		ResultSet rs = st.executeQuery(query);
		
		int count = 0;
		while(rs.next()) {
			View v = new View();
			v.setName( rs.getString(3) );
			v.setQuery( rs.getString(4) );
			Matcher m = patternLastSemicolon.matcher(v.getQuery());
			if(m.find()) {
				v.setQuery( m.replaceAll("") );
			}
			/*if(v.query!=null && v.query.endsWith(";")) {
				v.query = v.query.substring(0, v.query.length()-1);
			}*/
			v.setSchemaName( schemaPattern );
			String checkOption = rs.getString(5);
			if(checkOption!=null) {
				try {
					v.setCheckOption( View.CheckOptionType.valueOf(checkOption) ); //"YES".equalsIgnoreCase(rs.getString(5));
				}
				catch (Exception e) {
					log.warn("unknown check option: "+checkOption+" [view '"+v.getName()+"']");
				}
			}
			if(allowViewSetWithReadOnly() && !"YES".equalsIgnoreCase(rs.getString(6))) {
				v.setWithReadOnly(true);
			}
			views.add(v);
			count++;
		}
		
		rs.close();
		st.close();
		log.info("["+schemaPattern+"]: "+count+" views grabbed");
	}

	/*
	 * see: http://www.postgresql.org/docs/9.1/static/infoschema-triggers.html
	 */
	String grabDBTriggersQuery(String schemaPattern, String tableNamePattern, String triggerNamePattern) {
		return "select trigger_catalog, trigger_schema, trigger_name, event_manipulation, event_object_schema, event_object_table, action_statement, action_orientation, action_timing, action_condition "
			+"from information_schema.triggers "
			+"where trigger_schema = '"+schemaPattern+"' "
			+(tableNamePattern!=null?"and event_object_table = '"+tableNamePattern+"' ":"")
			+(triggerNamePattern!=null?"and trigger_name = '"+triggerNamePattern+"' ":"")
			+"order by trigger_catalog, trigger_schema, trigger_name, event_manipulation ";
	}
	
	/*
	 * TODOne: add when_clause - [ WHEN ( condition ) ] - see http://www.postgresql.org/docs/9.1/static/sql-createtrigger.html
	 */
	@Override
	public void grabDBTriggers(Collection<Trigger> triggers, String schemaPattern, String tableNamePattern, String triggerNamePattern, Connection conn) throws SQLException {
		log.debug("grabbing triggers");
		String query = grabDBTriggersQuery(schemaPattern, tableNamePattern, triggerNamePattern);
		Statement st = conn.createStatement();
		log.debug("sql: "+query);
		ResultSet rs = st.executeQuery(query);
		
		int count = 0;
		int rowcount = 0;
		while(rs.next()) {
			String schemaName = rs.getString(2);
			String name = rs.getString(3);
			InformationSchemaTrigger t = DBIdentifiable.getDBIdentifiableBySchemaAndName(triggers, schemaName, name);
			
			if(t==null) {
				t = new InformationSchemaTrigger();
				t.setSchemaName( schemaName );
				t.setName( name );
				triggers.add(t);
				count++;
			}
			t.eventsManipulation.add(rs.getString(4));
			t.setTableName(rs.getString(6));
			t.actionStatement = rs.getString(7);
			t.actionOrientation = rs.getString(8);
			t.conditionTiming = rs.getString(9);
			t.setWhenClause(rs.getString(10));
			rowcount++;
		}
		
		rs.close();
		st.close();
		log.info("["+schemaPattern+"]: "+count+" triggers grabbed [rowcount="+rowcount+"]");
	}

	String grabDBRoutinesQuery(String schemaPattern, String execNamePattern) {
		return "select routine_name, routine_type, r.data_type, external_language, routine_definition, p.parameter_name, p.data_type, p.ordinal_position "
				+"\nfrom "+informationSchema+".routines r left outer join "+informationSchema+".parameters p on r.specific_name = p.specific_name "
				+"\nwhere r.routine_definition is not null "
				+"and r.specific_schema = '"+schemaPattern+"' "
				+(execNamePattern!=null?"and routine_name = '"+execNamePattern+"' ":"")
				+"\norder by routine_catalog, routine_schema, routine_name, p.ordinal_position ";
	}
	
	@Override
	public void grabDBExecutables(Collection<ExecutableObject> execs, String schemaPattern, String execNamePattern, Connection conn) throws SQLException {
		log.debug("grabbing executables");
		String query = grabDBRoutinesQuery(schemaPattern, execNamePattern);
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
					if(addExecutableToModel(execs, eo)) {
						count++;
					}
				}

				//new object
				eo = new InformationSchemaRoutine();
				eo.setSchemaName( schemaPattern );
				eo.setName( routineName );
				try {
					String stype = rs.getString(2);
					eo.setType( DBObjectType.parse(stype) );
				}
				catch(IllegalArgumentException iae) {
					log.warn("grabDBExecutables: unknown object type: "+rs.getString(2));
					eo.setType( DBObjectType.EXECUTABLE );
				}
				catch(NullPointerException e) {
					log.warn("grabDBExecutables: null object type");
					eo.setType( DBObjectType.EXECUTABLE );
				}
				ExecutableParameter ep = new ExecutableParameter();
				ep.setDataType(rs.getString(3));
				eo.setReturnParam(ep);
				eo.setParams(new ArrayList<ExecutableParameter>());
				
				eo.externalLanguage = rs.getString(4);
				eo.setBody( rs.getString(5) );
			}
			ExecutableParameter ep = new ExecutableParameter();
			ep.setName(rs.getString(6));
			ep.setDataType(rs.getString(7));
			ep.setPosition(rs.getInt(8));
			
			// routine may have no parameters
			if(ep.getName()!=null || ep.getDataType()!=null) {
				eo.getParams().add(ep);
			}
		}
		if(eo!=null) {
			if(addExecutableToModel(execs, eo)) {
				count++;
			}
		}
		
		rs.close();
		st.close();
		log.info("["+schemaPattern+"]: "+count+" executable objects/routines grabbed");
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
	
	@Override
	public void grabDBSequences(Collection<Sequence> seqs, String schemaPattern, String sequenceNamePattern, Connection conn) throws SQLException {
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
				s.setMinValue(rs.getLong(2));
			}
			s.setIncrementBy(1); //rs.getLong(3);
			//s.lastNumber = rs.getLong(4);
			seqs.add(s);
			count++;
		}
		
		rs.close();
		st.close();
		log.info("["+schemaPattern+"]: "+count+" sequences grabbed");
	}

	String grabDBCheckConstraintsQuery(String schemaPattern, String tableNamePattern) {
		return "select cc.constraint_schema, table_name, cc.constraint_name, check_clause " 
				+"from information_schema.check_constraints cc, information_schema.constraint_column_usage ccu "
				+"where cc.constraint_name = ccu.constraint_name "
				+"and cc.constraint_schema = '"+schemaPattern+"' "
				+(tableNamePattern!=null?"and table_name = '"+tableNamePattern+"' ":"")
				+"order by table_name, constraint_name ";
	}
	
	@Override
	public void grabDBCheckConstraints(Collection<Table> tables, String schemaPattern, String tableNamePattern, String constraintNamePattern, Connection conn) throws SQLException {
		log.debug("grabbing check constraints");
		
		String query = grabDBCheckConstraintsQuery(schemaPattern, tableNamePattern);
		Statement st = conn.createStatement();
		log.debug("sql: "+query);
		ResultSet rs = st.executeQuery(query);
		
		int count = 0;
		int countConstraints = 0;
		while(rs.next()) {
			String schemaName = rs.getString(1);
			String tableName = rs.getString(2);
			
			Constraint c = new Constraint();
			c.setType(Constraint.ConstraintType.CHECK);
			c.setName( rs.getString(3) );
			c.setCheckDescription(rs.getString(4));
			Table t = DBIdentifiable.getDBIdentifiableBySchemaAndName(tables, schemaName, tableName);
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
		log.info("["+schemaPattern+"]: "+countConstraints+" check constraints grabbed [rowcount="+count+"]");
	}

	//order by "column_position"? see grabDBUniqueConstraints()
	//XXX use key_column_usage? see http://www.postgresql.org/docs/9.1/static/infoschema-key-column-usage.html
	String grabDBUniqueConstraintsQuery(String schemaPattern, String tableNamePattern, String constraintNamePattern) {
		return "select tc.constraint_schema, tc.table_name, tc.constraint_name, column_name " 
				+"from information_schema.table_constraints tc, information_schema.constraint_column_usage ccu "
				+"where tc.constraint_name = ccu.constraint_name "
				+"and tc.constraint_schema = '"+schemaPattern+"' "
				+"and constraint_type = 'UNIQUE' "
				+(tableNamePattern!=null?"and tc.table_name = '"+tableNamePattern+"' ":"")
				+(constraintNamePattern!=null?"and tc.constraint_name = '"+constraintNamePattern+"' ":"")
				+"order by table_name, constraint_name, column_name ";
	}
	
	@Override
	public void grabDBUniqueConstraints(Collection<Table> tables, String schemaPattern, String tableNamePattern, String constraintNamePattern, Connection conn) throws SQLException {
		log.debug("grabbing unique constraints");

		//XXX: table constraint_column_usage has no 'column_order' column... ordering by column name
		String query = grabDBUniqueConstraintsQuery(schemaPattern, tableNamePattern, constraintNamePattern);
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
				c.setType(Constraint.ConstraintType.UNIQUE);
				c.setName( constraintName );
				Table t = DBIdentifiable.getDBIdentifiableBySchemaAndName(tables, schemaName, tableName);
				if(t!=null) {
					t.getConstraints().add(c);
					countUniqueConstraints++;
				}
				else {
					log.warn("constraint "+c+" can't be added to table '"+tableName+"': table not found");
				}
			}
			c.getUniqueColumns().add(rs.getString(4));
			previousConstraintId = constraintId;
			count++;
		}
		
		rs.close();
		st.close();
		log.info("["+schemaPattern+"]: "+countUniqueConstraints+" unique constraints grabbed [colcount="+count+"]");
	}
	
	protected String getInformationSchemaName() {
		return informationSchema;
	}
	
	protected void setInformationSchemaName(String informationSchema) {
		this.informationSchema = informationSchema;
	}
	
	@Override
	public List<DBObjectType> getExecutableObjectTypes() {
		return Arrays.asList(execTypes);
	}
	
	@Override
	public List<DBObjectType> getSupportedObjectTypes() {
		return Arrays.asList(supportedTypes);
	}

	protected boolean allowViewSetWithReadOnly() {
		return true;
	}
	
}
