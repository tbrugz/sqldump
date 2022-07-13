package tbrugz.sqldump.dbmsfeatures;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Collection;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import tbrugz.sqldump.dbmodel.Column;
import tbrugz.sqldump.dbmodel.Constraint;
import tbrugz.sqldump.dbmodel.DBIdentifiable;
import tbrugz.sqldump.dbmodel.DBObject;
import tbrugz.sqldump.dbmodel.NamedDBObject;
import tbrugz.sqldump.dbmodel.Table;
import tbrugz.sqldump.util.StringUtils;

/*
 * Use this features class for H2 version >= 2.0.202
 *
 * see: https://www.h2database.com/html/systemtables.html
 */
public class H2Features extends InformationSchemaFeatures {

	static final Log log = LogFactory.getLog(H2Features.class);

	@Override
	String grabDBTriggersQuery(String schemaPattern, String tableNamePattern, String triggerNamePattern) {
		// other columns: REMARKS, SQL, QUEUE_SIZE, NO_WAIT, ID
		return "select trigger_catalog, trigger_schema, trigger_name, event_manipulation, event_object_schema, event_object_table "
				+"  , 'call \"'||java_class||'\"' as action_statement "
				+"  , action_orientation, action_timing, null as action_condition "
				+"from information_schema.triggers "
				+"where trigger_schema = '"+schemaPattern+"' "
				+(tableNamePattern!=null?"and event_object_table = '"+tableNamePattern+"' ":"")
				+(triggerNamePattern!=null?"and trigger_name = '"+triggerNamePattern+"' ":"")
				+"order by trigger_catalog, trigger_schema, trigger_name, event_manipulation";
	}

	String grabDBRoutinesQuery(String schemaPattern, String execNamePattern) {
		return "select routine_name, routine_type, r.data_type, external_language, "
				//+"routine_definition, "
				+"\ncase "
				+"when routine_type='FUNCTION' and routine_definition is not null then 'create alias '||routine_name||(case is_deterministic when 'YES' then ' deterministic' else '' end)||' as $$'||routine_definition||'$$;' "
				+"when routine_type='FUNCTION' and external_name is not null then 'create alias '||routine_name||(case is_deterministic when 'YES' then ' deterministic' else '' end)||' for \"'||external_name||'\";' "
				+"when routine_type='AGGREGATE' and external_name is not null then 'create aggregate '||routine_name||' for \"'||external_name||'\";' "
				+"else routine_definition end as routine_definition, "
				+"\nexternal_name, is_deterministic, "
				+"p.parameter_name, p.data_type, p.ordinal_position "
				+"\nfrom "+informationSchema+".routines r left outer join "+informationSchema+".parameters p on r.specific_name = p.specific_name "
				+"\nwhere 1=1 "
				+"and (r.routine_definition is not null or external_name is not null)"
				+"and r.specific_schema = '"+schemaPattern+"' "
				+(execNamePattern!=null?"and routine_name = '"+execNamePattern+"' ":"")
				+"\norder by routine_catalog, routine_schema, routine_name, p.ordinal_position ";
	}

	/*
	 * create alias: http://www.h2database.com/html/grammar.html#create_alias
	 * create aggregate: http://www.h2database.com/html/grammar.html#create_aggregate
	 * 
	 * INFORMATION_SCHEMA: FUNCTION_ALIASES, FUNCTION_COLUMNS
	 */
	 /*
	@Override
	String grabDBRoutinesQuery(String schemaPattern, String execNamePattern) {
		return "select r.alias_name as routine_name, "+
				//"null as routine_type, "+
				"  case "+
				// create alias as
				"    when r.source is not null and r.source != '' then 'EXECUTABLE' "+ //XXX: ALIAS?
				// create alias for
				"    when r.java_class is not null and r.java_class != '' and r.java_method is not null and r.java_method != '' then 'EXECUTABLE' "+ //XXX: ALIAS?
				// create aggregate
				"    when r.java_class is not null and (r.java_method is null or r.java_method = '') then 'AGGREGATE' "+
				// else...
				"  end as routine_type, "+
				"r.type_name as data_type, null as external_language,"+
				//" r.java_class||'#'||r.java_method as routine_definition, "+
				"  case "+
				// create alias as
				"    when r.source is not null and r.source != '' then 'create alias '||r.alias_name||' as $$'||r.source||'$$;' "+
				// create alias for
				"    when r.java_class is not null and r.java_class != '' and r.java_method is not null and r.java_method != '' then 'create alias '||r.alias_name||' for \"'||r.java_class||'.'||r.java_method||'\";' "+
				// create aggregate
				"    when r.java_class is not null and (r.java_method is null or r.java_method = '') then 'create aggregate '||r.alias_name||' for \"'||r.java_class||'\";' "+
				// else...
				"    else r.java_class||'#'||r.java_method||'#'||r.source "+ //???
				"  end as routine_definition, "+
				" p.column_name as parameter_name, p.type_name as data_type, p.pos as ordinal_position "+
				"\nfrom "+informationSchema+".function_aliases r left outer join "+informationSchema+".function_columns p on r.alias_name = p.alias_name "+
				"\nwhere 1=1 "+
				" and r.alias_schema = '"+schemaPattern+"' "+
				(execNamePattern!=null?"and r.alias_name = '"+execNamePattern+"' ":"")+
				"\norder by r.alias_catalog, r.alias_schema, r.alias_name, p.pos ";
	}
	*/

	/*
	@Override
	String grabDBSequencesQuery(String schemaPattern) {
		return "select sequence_name, null as minimum_value, increment, null as maximum_value "
				+"from information_schema.sequences "
				+"where sequence_schema = '"+schemaPattern+"' "
				+"order by sequence_catalog, sequence_schema, sequence_name ";
	}
	*/
	
	/*
	@Override
	String grabDBCheckConstraintsQuery(String schemaPattern, String tableNamePattern) {
		return "select cc.constraint_schema, table_name, cc.constraint_name, check_expression as check_clause " 
			+ "from information_schema.constraints cc "
			+ "where cc.constraint_schema = '"+schemaPattern+"'"
			+ "and constraint_type = 'CHECK'"
			+(tableNamePattern!=null?"and table_name = '"+tableNamePattern+"' ":"")
			+ "order by table_name, constraint_name";
	}
	*/
	
	/*
	@Override
	String grabDBUniqueConstraintsQuery(String schemaPattern, String tableNamePattern, String constraintNamePattern) {
		return "select tc.constraint_schema, tc.table_name, tc.constraint_name, column_list " 
				+"from information_schema.constraints tc "
				+"where constraint_type = 'UNIQUE' "
				+(schemaPattern!=null?"and tc.constraint_schema = '"+schemaPattern+"' ":"")
				+(tableNamePattern!=null?"and tc.table_name = '"+tableNamePattern+"' ":"")
				+(constraintNamePattern!=null?"and tc.constraint_name = '"+constraintNamePattern+"' ":"")
				+"order by table_name, constraint_name ";
	}

	@Override
	public void grabDBUniqueConstraints(Collection<Table> tables, String schemaPattern, String tableNamePattern, String constraintNamePattern, Connection conn) throws SQLException {
		log.debug("grabbing unique constraints");
		String query = grabDBUniqueConstraintsQuery(schemaPattern, tableNamePattern, constraintNamePattern);
		Statement st = conn.createStatement();
		log.debug("sql: "+query);
		ResultSet rs = st.executeQuery(query);
		
		int countUKs = 0;
		int countCols = 0;
		while(rs.next()) {
			String schemaName = rs.getString(1);
			String tableName = rs.getString(2);
			
			Constraint c = new Constraint();
			c.setType(Constraint.ConstraintType.UNIQUE);
			c.setName( rs.getString(3) );
			String columnList = rs.getString(4);
			String[] cols = columnList.split(",");
			for(String ss: cols) {
				c.getUniqueColumns().add(ss.trim());
			}
			Table t = DBIdentifiable.getDBIdentifiableBySchemaAndName(tables, schemaName, tableName);
			if(t!=null) {
				countCols += cols.length;
				countUKs++;
				t.getConstraints().add(c);
			}
			else {
				log.warn("constraint "+c+" can't be added to table '"+tableName+"': table not found");
			}
		}
		
		rs.close();
		st.close();
		log.info(countUKs+" unique constraints grabbed [colcount="+countCols+"]");
	}
	*/
	
	@Override
	public String sqlRenameColumnDefinition(NamedDBObject table, Column column, String newName) {
		return "alter table "+DBObject.getFinalName(table, true)+" alter column "+DBObject.getFinalIdentifier(column.getName())
				+" rename to "+DBObject.getFinalIdentifier(newName);
	}
	
	@Override
	public boolean supportsDiffingColumn() {
		return true;
	}
	/*
	@Override
	public String sqlAlterColumnByDiffing(NamedDBObject table, Column previousColumn, Column column) {
		//see: http://www.h2database.com/html/grammar.html#alter_table_alter_column
		if(! StringUtils.equalsWithUpperCase(previousColumn.getTypeDefinition(), column.getTypeDefinition())) {
			return createAlterColumn(table, column, " "+column.getTypeDefinition());
			//XXX add default & not null besides type?
		}
		else if(!previousColumn.getDefaultSnippet().equals(column.getDefaultSnippet())) {
			return createAlterColumn(table, column,
					" set"+column.getFullDefaultSnippet()
					//" set default "+
					//(column.getDefaultSnippet().trim().equals("")?"null":column.getDefaultValue() )
					);
		}
		else if(!previousColumn.getNullableSnippet().equals(column.getNullableSnippet())) {
			return createAlterColumn(table, column,
					" set"+column.getFullNullableSnippet()
					//" set "+(column.isNullable()?"null":"not null")
					);
		}
		
		log.warn("no differences between H2 columns found");
		return null;
		//throw new UnsupportedOperationException("no differences between H2 columns found");
	}
	@Override
	public String sqlAlterColumnByDiffing(NamedDBObject table, Column previousColumn, Column column) {
		//see: http://www.h2database.com/html/grammar.html#alter_table_alter_column
		
		String alterSql = "";
		if(! StringUtils.equalsWithUpperCase(previousColumn.getTypeDefinition(), column.getTypeDefinition())) {
			alterSql += " "+column.getTypeDefinition();
		}
		else if(!previousColumn.getDefaultSnippet().equals(column.getDefaultSnippet())) {
			alterSql += " set"+column.getFullDefaultSnippet();
		}
		else if(!previousColumn.getNullableSnippet().equals(column.getNullableSnippet())) {
			alterSql += " set"+column.getFullNullableSnippet();
		}
		
		if(!alterSql.trim().equals("")) {
			alterSql = DiffUtil.createAlterColumn(this, table, column, alterSql);
		}
		else {
			alterSql = null;
		}
		
		log.warn("no differences between H2 columns found");
		return alterSql;
		//throw new UnsupportedOperationException("no differences between H2 columns found");
	}
	*/
	
	@Override
	public String sqlAlterColumnByDiffing(Column previousColumn, Column column) {
		//String alterSql = "";
		if(! StringUtils.equalsWithUpperCase(previousColumn.getTypeDefinition(), column.getTypeDefinition())) {
			return " "+column.getTypeDefinition();
		}
		else if(!previousColumn.getDefaultSnippet().equals(column.getDefaultSnippet())) {
			return " set"+column.getFullDefaultSnippet();
		}
		else if(!previousColumn.getNullableSnippet().equals(column.getNullableSnippet())) {
			return " set"+column.getFullNullableSnippet();
		}
		
		//if(alterSql.trim().equals("")) {
		//	alterSql = null;
		//}
		//return alterSql;
		return null;
	}
	
	@Override
	public boolean supportsExplainPlan() {
		return true;
	}
	
	/*
	 * http://www.h2database.com/html/grammar.html#explain
	 */
	@Override
	public ResultSet explainPlan(String sql, List<Object> params, Connection conn) throws SQLException {
		String expsql = sqlExplainPlanQuery(sql);
		return bindAndExecuteQuery(expsql, params, conn);
	}
	
	@Override
	public String sqlExplainPlanQuery(String sql) {
		return "explain plan for "+sql;
	}
	
	@Override
	public boolean supportsAddColumnAfter() {
		return true;
	}
	
	@Override
	public boolean supportsCreateIndexWithoutName() {
		return true;
	}
	
	@Override
	public String sqlIsNullFunction(String columnName) {
		return columnName+" is null";
	}

	@Override
	public String sqlLengthFunctionByType(String columnName, String columnType) {
		return "length("+columnName+")";
	}
	
	protected boolean allowViewSetWithReadOnly() {
		return false;
	}

}
