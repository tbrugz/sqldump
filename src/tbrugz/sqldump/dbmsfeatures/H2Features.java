package tbrugz.sqldump.dbmsfeatures;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Collection;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import tbrugz.sqldump.dbmodel.Column;
import tbrugz.sqldump.dbmodel.Constraint;
import tbrugz.sqldump.dbmodel.DBIdentifiable;
import tbrugz.sqldump.dbmodel.DBObject;
import tbrugz.sqldump.dbmodel.ExecutableObject;
import tbrugz.sqldump.dbmodel.NamedDBObject;
import tbrugz.sqldump.dbmodel.Table;

/*
 * see: http://www.h2database.com/html/grammar.html#information_schema
 */
public class H2Features extends InformationSchemaFeatures {
	static Log log = LogFactory.getLog(H2Features.class);

	@Override
	String grabDBTriggersQuery(String schemaPattern) {
		// other columns: REMARKS, SQL, QUEUE_SIZE, NO_WAIT, ID
		return "select trigger_catalog, trigger_schema, trigger_name, lower(trigger_type) as event_manipulation, null as event_object_schema, table_name as event_object_table "
				+"  , 'call \"'||java_class||'\"' as action_statement "
				+"  , 'row' as action_orientation, casewhen(before, 'before', 'after') as action_timing, null as action_condition "
				+"from information_schema.triggers "
				+"where trigger_schema = '"+schemaPattern+"' "
				+"order by trigger_catalog, trigger_schema, trigger_name, event_manipulation";
	}
	
	@Override
	public void grabDBExecutables(Collection<ExecutableObject> execs, String schemaPattern, String execNamePattern, Connection conn) throws SQLException {
		log.debug("grab routines: not supported"); //warn level?
	}
	
	@Override
	String grabDBSequencesQuery(String schemaPattern) {
		return "select sequence_name, null as minimum_value, increment, null as maximum_value "
				+"from information_schema.sequences "
				+"where sequence_schema = '"+schemaPattern+"' "
				+"order by sequence_catalog, sequence_schema, sequence_name ";
	}
	
	@Override
	String grabDBCheckConstraintsQuery(String schemaPattern) {
		return "select cc.constraint_schema, table_name, cc.constraint_name, check_expression as check_clause " 
			+ "from information_schema.constraints cc "
			+ "where cc.constraint_schema = '"+schemaPattern+"'"
			+ "and constraint_type = 'CHECK'"
			+ "order by table_name, constraint_name";
	}
	
	
	@Override
	String grabDBUniqueConstraintsQuery(String schemaPattern, String constraintNamePattern) {
		return "select tc.constraint_schema, tc.table_name, tc.constraint_name, column_list " 
				+"from information_schema.constraints tc "
				+"where constraint_type = 'UNIQUE' "
				+(constraintNamePattern!=null?"and tc.constraint_name = '"+constraintNamePattern+"' ":"")
				+"order by table_name, constraint_name ";
	}

	@Override
	public void grabDBUniqueConstraints(Collection<Table> tables, String schemaPattern, String constraintNamePattern, Connection conn) throws SQLException {
		log.debug("grabbing unique constraints");
		String query = grabDBUniqueConstraintsQuery(schemaPattern, constraintNamePattern);
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
			countCols += cols.length;
			Table t = DBIdentifiable.getDBIdentifiableBySchemaAndName(tables, schemaName, tableName);
			if(t!=null) {
				t.getConstraints().add(c);
			}
			else {
				log.warn("constraint "+c+" can't be added to table '"+tableName+"': table not found");
			}
			countUKs++;
		}
		
		rs.close();
		st.close();
		log.info(countUKs+" unique constraints grabbed [colcount="+countCols+"]");
	}
	
	@Override
	public String sqlRenameColumnDefinition(NamedDBObject table, Column column,
			String newName) {
		return "alter table "+DBObject.getFinalName(table, true)+" alter column "+DBObject.getFinalIdentifier(column.getName())
				+" rename to "+DBObject.getFinalIdentifier(newName);
	}
	
	@Override
	public boolean supportsDiffingColumn() {
		return true;
	}

	@Override
	public String sqlAlterColumnByDiffing(NamedDBObject table, Column previousColumn,
			Column column) {
		//see: http://www.h2database.com/html/grammar.html#alter_table_alter_column
		if(!previousColumn.getTypeDefinition().equals(column.getTypeDefinition())) {
			return createAlterColumn(table, column,
					" "+column.getTypeDefinition());
			//XXX add default & not null besides type?
		}
		else if(!previousColumn.getDefaultSnippet().equals(column.getDefaultSnippet())) {
			return createAlterColumn(table, column,
					" set default "+(column.getDefaultSnippet().trim().equals("")?"null":column.getDefaultValue()));
		}
		else if(!previousColumn.getNullableSnippet().equals(column.getNullableSnippet())) {
			return createAlterColumn(table, column,
					" set "+(column.isNullable()?"null":"not null"));
		}
		
		throw new UnsupportedOperationException("no differences between H2 columns found");
	}
	
}
