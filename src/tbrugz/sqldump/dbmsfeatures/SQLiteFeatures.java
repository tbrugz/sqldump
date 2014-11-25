package tbrugz.sqldump.dbmsfeatures;

import java.sql.Connection;
import java.sql.SQLException;

import tbrugz.sqldump.dbmodel.SchemaModel;

/*
 * using: http://code.google.com/p/sqlite-jdbc/ 
 * was using: http://www.zentus.com/sqlitejdbc/
 * 
 * see:
 *   http://www.sqlite.org/cvstrac/wiki?p=InformationSchema
 *   http://www.sqlite.org/pragma.html
 * 
 * XXXold: MetaData.getImportedKeys(MetaData.java:503): not yet implemented...
 *   - PRAGMA foreign_key_list(fkTable);
 * XXXold: MetaData.getIndexInfo(MetaData.java:506): not yet implemented...
 *   - PRAGMA index_list(table);
 *   - PRAGMA index_info(indexName);
 * 
 * ? MetaData.getTablePrivileges() ?
 */
public class SQLiteFeatures extends InformationSchemaFeatures {
	
	@Override
	String grabDBViewsQuery(String schemaPattern) {
		//return "select 'main' as table_catalog, 'sqlite' as table_schema, tbl_name as table_name, sql as view_definition "
		return "select null as table_catalog, null as table_schema, tbl_name as table_name, sql as view_definition "
				+"from sqlite_master "
				+"where type = 'view' "
				+"order by table_catalog, table_schema, table_name";
	}
	
	@Override
	public void grabDBTriggers(SchemaModel model, String schemaPattern, String tableNamePattern, String triggerNamePattern, Connection conn) throws SQLException {
	}
	
	@Override
	public void grabDBExecutables(SchemaModel model, String schemaPattern, String execNamePattern, Connection conn) throws SQLException {
	}
	
	@Override
	public void grabDBSequences(SchemaModel model, String schemaPattern, String sequenceNamePattern, Connection conn) throws SQLException {
	}
	
	@Override
	public void grabDBCheckConstraints(SchemaModel model, String schemaPattern, String constraintNamePattern, Connection conn) throws SQLException {
	}
	
	@Override
	public void grabDBUniqueConstraints(SchemaModel model, String schemaPattern, String constraintNamePattern, Connection conn) throws SQLException {
	}
	
}
