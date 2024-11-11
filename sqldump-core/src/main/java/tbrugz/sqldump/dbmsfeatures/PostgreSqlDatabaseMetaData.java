package tbrugz.sqldump.dbmsfeatures;

import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import tbrugz.sqldump.dbmodel.QueryWithParams;

public class PostgreSqlDatabaseMetaData extends InformationSchemaDatabaseMetaData {

	final int majorVersion;
	
	public PostgreSqlDatabaseMetaData(DatabaseMetaData metadata) throws SQLException {
		super(metadata);
		this.majorVersion = metadata.getDatabaseMajorVersion();
	}
	
	/*
	 * see: https://stackoverflow.com/questions/5664094/getting-list-of-table-comments-in-postgresql
	 * 
	 * pg_class.relispartition exists in postgresql 10+
	 * https://www.postgresql.org/docs/10/catalog-pg-class.html
	 */
	@Override
	QueryWithParams getTablesQuery(String schemaPattern, String tableNamePattern) {
		//return super.getTablesQuery(schemaPattern, tableNamePattern);
		String sql = "select table_catalog as table_cat, table_schema as table_schem, table_name,\n"
				//+ "	table_type,\n"
				+ "	case when relkind = 'p' then 'PARTITIONED TABLE' "
				+ "		when pgc.relispartition then 'TABLE PARTITION'"
				+ "		when table_type = 'BASE TABLE' then 'TABLE'" //??
				+ "		else table_type end as table_type,\n"
				+ "	pg_catalog.obj_description(pgc.oid, 'pg_class') as remarks,\n"
				+ "	user_defined_type_catalog as type_cat, user_defined_type_schema as type_schem, user_defined_type_name as type_name,\n"
				+ "	self_referencing_column_name, reference_generation as ref_generation\n"
				+ "from information_schema.tables t\n"
				+ "join pg_catalog.pg_class pgc on t.table_name = pgc.relname\n"
				+ "where 1=1\n";
		List<Object> params = new ArrayList<>();
		if(schemaPattern!=null) {
			sql += "and table_schema like ?\n";
			params.add(schemaPattern);
		}
		if(tableNamePattern!=null) {
			sql += "and table_name like ?\n";
			params.add(tableNamePattern);
		}
		return new QueryWithParams(sql, params);
	}
	
	@Override
	public ResultSet getTables(String catalog, String schemaPattern, String tableNamePattern, String[] types)
			throws SQLException {
		if(majorVersion>=10) {
			return getTablesInternal(catalog, schemaPattern, tableNamePattern, types);
		}
		return super.getTables(catalog, schemaPattern, tableNamePattern, types);
	}

}
