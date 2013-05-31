package tbrugz.sqldump.dbmsfeatures;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import tbrugz.sqldump.dbmodel.Constraint;
import tbrugz.sqldump.dbmodel.DBIdentifiable;
import tbrugz.sqldump.dbmodel.SchemaModel;
import tbrugz.sqldump.dbmodel.Table;

/*
 * see: http://www.h2database.com/html/grammar.html#information_schema
 */
public class H2Features extends InformationSchemaFeatures {
	static Log log = LogFactory.getLog(H2Features.class);

	@Override
	void grabDBTriggers(SchemaModel model, String schemaPattern, Connection conn) throws SQLException {
		log.warn("grab triggers: not implemented");
	}
	
	@Override
	void grabDBRoutines(SchemaModel model, String schemaPattern, Connection conn) throws SQLException {
		log.warn("grab routines: not implemented");
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
	String grabDBUniqueConstraintsQuery(String schemaPattern) {
		return "select tc.constraint_schema, tc.table_name, tc.constraint_name, column_list " 
				+"from information_schema.constraints tc "
				+"where constraint_type = 'UNIQUE' "
				+"order by table_name, constraint_name ";
	}

	@Override
	void grabDBUniqueConstraints(SchemaModel model, String schemaPattern, Connection conn) throws SQLException {
		log.debug("grabbing unique constraints");
		String query = grabDBUniqueConstraintsQuery(schemaPattern);
		Statement st = conn.createStatement();
		log.debug("sql: "+query);
		ResultSet rs = st.executeQuery(query);
		
		int countUKs = 0;
		int countCols = 0;
		while(rs.next()) {
			String schemaName = rs.getString(1);
			String tableName = rs.getString(2);
			
			Constraint c = new Constraint();
			c.type = Constraint.ConstraintType.UNIQUE;
			c.setName( rs.getString(3) );
			String columnList = rs.getString(4);
			String[] cols = columnList.split(",");
			for(String ss: cols) {
				c.uniqueColumns.add(ss.trim());				
			}
			countCols += cols.length;
			Table t = DBIdentifiable.getDBIdentifiableBySchemaAndName(model.getTables(), schemaName, tableName);
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
}
