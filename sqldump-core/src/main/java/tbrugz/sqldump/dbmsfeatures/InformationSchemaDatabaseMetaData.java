package tbrugz.sqldump.dbmsfeatures;

import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import tbrugz.sqldump.dbmd.AbstractDatabaseMetaDataDecorator;
import tbrugz.sqldump.dbmodel.QueryWithParams;

public class InformationSchemaDatabaseMetaData extends AbstractDatabaseMetaDataDecorator {

	static final Log log = LogFactory.getLog(InformationSchemaDatabaseMetaData.class);
	
	boolean xtraColumnInfoAvailable = false;
	
	public InformationSchemaDatabaseMetaData(DatabaseMetaData metadata) {
		super(metadata);
	}
	
	QueryWithParams getTablesQuery(String schemaPattern, String tableNamePattern) {
		String sql = "select table_catalog as table_cat, table_schema as table_schem, table_name,\n"+
				"table_type, null as remarks,\n"+
				"user_defined_type_catalog as type_cat, user_defined_type_schema as type_schem, user_defined_type_name as type_name,\n"+
				"self_referencing_column_name, reference_generation as ref_generation\n"+
				"from information_schema.tables t\n"+
				"where 1=1\n";
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
	
	ResultSet getTablesInternal(String catalog, String schemaPattern, String tableNamePattern, String[] types)
			throws SQLException {
		QueryWithParams qwp = getTablesQuery(schemaPattern, tableNamePattern);
		log.debug("getTablesInternal: sql: "+qwp);
		PreparedStatement st = getConnection().prepareStatement(qwp.getQuery());
		qwp.setParameters(st);
		return st.executeQuery();
	}

	/*
	 * DatabaseMetaData.getColumns(String catalog, String schemaPattern, String tableNamePattern, String columnNamePattern)
	 * see: https://docs.oracle.com/javase/8/docs/api/java/sql/DatabaseMetaData.html#getColumns-java.lang.String-java.lang.String-java.lang.String-java.lang.String-
	 */
	QueryWithParams getColumnsQuery(String schemaPattern, String tableNamePattern, String columnNamePattern) {
		String sql = "select\n"
				+ "    TABLE_CATALOG as TABLE_CAT, TABLE_SCHEMA as TABLE_SCHEM, TABLE_NAME, COLUMN_NAME,\n"
				+ "    null as DATA_TYPE, /* numeric JDBC type... */\n"
				+ "    DATA_TYPE as TYPE_NAME,\n"
				+ "    coalesce(NUMERIC_PRECISION, CHARACTER_MAXIMUM_LENGTH) as COLUMN_SIZE,\n"
				+ "    null as BUFFER_LENGTH,\n"
				+ "    coalesce(NUMERIC_SCALE, 0) as DECIMAL_DIGITS, \n"
				+ "    NUMERIC_PRECISION_RADIX as NUM_PREC_RADIX,\n"
				+ "    case when IS_NULLABLE='YES' then 1 else 0 end as NULLABLE,\n"
				+ "    REMARKS,\n"
				+ "    COLUMN_DEFAULT as COLUMN_DEF,\n"
				+ "    null as SQL_DATA_TYPE, null as SQL_DATETIME_SUB,\n"
				+ "    coalesce(CHARACTER_OCTET_LENGTH, NUMERIC_PRECISION) as CHAR_OCTET_LENGTH,\n"
				+ "    ORDINAL_POSITION, IS_NULLABLE,\n"
				+ "    null as SCOPE_CATALOG, null as SCOPE_SCHEMA, null as SCOPE_TABLE,\n"
				+ "    DECLARED_DATA_TYPE as SOURCE_DATA_TYPE,\n"
				+ "    IS_IDENTITY as IS_AUTOINCREMENT,\n"
				+ "    case when IS_GENERATED='NEVER' then 'NO' else 'YES' end as IS_GENERATEDCOLUMN,\n"
				+ "    IS_IDENTITY, IDENTITY_GENERATION, --IDENTITY_START, IDENTITY_INCREMENT, IDENTITY_MAXIMUM, IDENTITY_MINIMUM, IDENTITY_CYCLE,\n"
				+ "    IS_GENERATED, /* NEVER, ALWAYS */ \n"
				+ "    GENERATION_EXPRESSION\n"
				+ "from information_schema.columns\n"
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
		if(columnNamePattern!=null) {
			sql += "and column_name like ?\n";
			params.add(columnNamePattern);
		}
		return new QueryWithParams(sql, params);
	}

	ResultSet getColumnsInternal(String catalog, String schemaPattern, String tableNamePattern, String columnNamePattern)
			throws SQLException {
		QueryWithParams qwp = getColumnsQuery(schemaPattern, tableNamePattern, columnNamePattern);
		log.debug("getColumnsInternal: sql: "+qwp);
		PreparedStatement st = getConnection().prepareStatement(qwp.getQuery());
		qwp.setParameters(st);
		return st.executeQuery();
	}
	
	@Override
	public ResultSet getColumns(String catalog, String schemaPattern, String tableNamePattern, String columnNamePattern)
			throws SQLException {
		if(isXtraColumnInfoAvailable()) {
			return getColumnsInternal(catalog, schemaPattern, tableNamePattern, columnNamePattern);
		}
		return super.getColumns(catalog, schemaPattern, tableNamePattern, columnNamePattern);
	}
	
	public boolean isXtraColumnInfoAvailable() {
		return xtraColumnInfoAvailable;
	}
	
}
