package tbrugz.sqldump.dbmsfeatures;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import tbrugz.sqldump.SchemaModel;
import tbrugz.sqldump.dbmodel.Constraint;
import tbrugz.sqldump.dbmodel.DBObject;
import tbrugz.sqldump.dbmodel.Table;

/*
 * see: http://www.h2database.com/html/grammar.html#information_schema
 */
public class H2Features extends InformationSchemaFeatures {

	@Override
	void grabDBTriggers(SchemaModel model, String schemaPattern, Connection conn) throws SQLException {
		log.warn("grab triggers: not implemented");
	}
	
	@Override
	void grabDBRoutines(SchemaModel model, String schemaPattern, Connection conn) throws SQLException {
		log.warn("grab routines: not implemented");
	}
	
	@Override
	void grabDBSequences(SchemaModel model, String schemaPattern, Connection conn) throws SQLException {
		log.warn("grab sequences: not implemented");
	}
	
	@Override
	void grabDBCheckConstraints(SchemaModel model, String schemaPattern, Connection conn) throws SQLException {
		log.warn("grab check constraints: not implemented");
	}
	
	@Override
	String grabDBUniqueConstraintsQuery() {
		return "select tc.constraint_schema, tc.table_name, tc.constraint_name, column_list " 
				+"from information_schema.constraints tc "
				+"where constraint_type = 'UNIQUE' "
				+"order by table_name, constraint_name ";
	}

	@Override
	void grabDBUniqueConstraints(SchemaModel model, String schemaPattern, Connection conn) throws SQLException {
		log.debug("grabbing unique constraints");
		String query = grabDBUniqueConstraintsQuery();
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
			Table t = (Table) DBObject.findDBObjectBySchemaAndName(model.getTables(), schemaName, tableName);
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
