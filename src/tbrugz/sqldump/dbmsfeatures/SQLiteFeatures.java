package tbrugz.sqldump.dbmsfeatures;

import java.sql.Connection;
import java.sql.SQLException;

import tbrugz.sqldump.SchemaModel;

/*
 * using http://www.zentus.com/sqlitejdbc/
 * 
 * see: http://www.sqlite.org/cvstrac/wiki?p=InformationSchema
 * 
 * TODO: MetaData.getImportedKeys(): not yet implemented...
 */
public class SQLiteFeatures extends InformationSchemaFeatures {
	
	@Override
	String grabDBViewsQuery() {
		return "select 'main' as table_catalog, 'sqlite' as table_schema, tbl_name as table_name, sql as view_definition "
				+"from sqlite_master "
				+"where type = 'view' "
				+"order by table_catalog, table_schema, table_name";
	}
	
	@Override
	void grabDBTriggers(SchemaModel model, String schemaPattern, Connection conn) throws SQLException {
	}
	
	@Override
	void grabDBRoutines(SchemaModel model, String schemaPattern, Connection conn) throws SQLException {
	}
	
	@Override
	void grabDBSequences(SchemaModel model, String schemaPattern, Connection conn) throws SQLException {
	}
	
	@Override
	void grabDBCheckConstraints(SchemaModel model, String schemaPattern, Connection conn) throws SQLException {
	}
	
	@Override
	void grabDBUniqueConstraints(SchemaModel model, String schemaPattern, Connection conn) throws SQLException {
	}
	
}
