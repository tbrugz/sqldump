package tbrugz.sqldump.dbmsfeatures;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Collection;
import java.util.List;

import tbrugz.sqldump.dbmodel.Column;
import tbrugz.sqldump.dbmodel.DBObject;
import tbrugz.sqldump.dbmodel.NamedDBObject;
import tbrugz.sqldump.dbmodel.Sequence;
import tbrugz.sqldump.dbmodel.Table;

public class MySQLFeatures extends InformationSchemaFeatures {

	@Override
	String grabDBRoutinesQuery(String schemaPattern, String execNamePattern) {
		return "select routine_name, routine_type, '' as data_type, external_language, routine_definition "
				+"from information_schema.routines "
				+"where routine_definition is not null "
				+(execNamePattern!=null?"and routine_name = '"+execNamePattern+"' ":"")
				+"order by routine_catalog, routine_schema, routine_name ";
	}
	
	@Override
	public void grabDBSequences(Collection<Sequence> seqs, String schemaPattern, String sequenceNamePattern, Connection conn) throws SQLException {
	}
	
	/*
	 * check constraints seems not to be supported by mysql
	 * suggestion: using before-triggers: http://forums.mysql.com/read.php?136,152474,240479#msg-240479
	 */
	@Override
	public void grabDBCheckConstraints(Collection<Table> tables, String schemaPattern, String tableNamePattern, String constraintNamePattern, Connection conn) throws SQLException {
	}
	
	//XXX: see in information_schema: referential_constraints ; table_constraints -> PK, UNIQUE
	/*
	CREATE TEMPORARY TABLE `TABLE_CONSTRAINTS` (
		`CONSTRAINT_CATALOG` varchar(512) DEFAULT NULL,
		`CONSTRAINT_SCHEMA` varchar(64) NOT NULL DEFAULT '',
		`CONSTRAINT_NAME` varchar(64) NOT NULL DEFAULT '',
		`TABLE_SCHEMA` varchar(64) NOT NULL DEFAULT '',
		`TABLE_NAME` varchar(64) NOT NULL DEFAULT '',
		`CONSTRAINT_TYPE` varchar(64) NOT NULL DEFAULT ''
	) ENGINE=MEMORY DEFAULT CHARSET=utf8
	 */
	@Override
	String grabDBUniqueConstraintsQuery(String schemaPattern, String constraintNamePattern) {
		return "select tc.constraint_schema, tc.table_name, tc.constraint_name, column_name "
			+"from information_schema.table_constraints tc, information_schema.key_column_usage ccu "
			+"where tc.constraint_name = ccu.constraint_name "
			+"and tc.table_name = ccu.table_name " 
			+"and constraint_type = 'UNIQUE' "
			+(constraintNamePattern!=null?"and tc.constraint_name = '"+constraintNamePattern+"' ":"")
			+"order by table_name, constraint_name, column_name";
	}
	
	@Override
	public DatabaseMetaData getMetadataDecorator(DatabaseMetaData metadata) {
		return new MySQLDatabaseMetaData(metadata);
	}
	
	/*
	 * http://stackoverflow.com/questions/4002340/how-to-rename-a-table-column-in-mysql
	 */
	@Override
	public String sqlRenameColumnDefinition(NamedDBObject table, Column column, String newName) {
		return "alter table "+DBObject.getFinalName(table, true)+" change "+DBObject.getFinalIdentifier(column.getName())
				+" "+DBObject.getFinalIdentifier(newName)+" "+column.getTypeDefinition();
	}
	
	@Override
	public boolean supportsExplainPlan() {
		return true;
	}
	
	/*
	 * https://dev.mysql.com/doc/refman/5.0/en/explain.html
	 * https://dev.mysql.com/doc/refman/5.0/en/explain-output.html
	 */
	@Override
	public ResultSet explainPlan(String sql, List<Object> params, Connection conn) throws SQLException {
		String expsql = sqlExplainPlanQuery(sql);
		return bindAndExecuteQuery(expsql, params, conn);
	}
	
	@Override
	public String sqlExplainPlanQuery(String sql) {
		return "explain "+sql;
	}
	
	@Override
	public String getIdentifierQuoteString() {
		return "`";
	}
	
	@Override
	public String sqlAlterColumnClause() {
		return "modify";
	}
	
	@Override
	public boolean supportsAddColumnAfter() {
		return true;
	}
	
	@Override
	public boolean alterColumnTypeRequireFullDefinition() {
		return true;
	}
	
	@Override
	public String sqlLengthFunctionByType(String columnName, String columnType) {
		return "length("+columnName+")";
	}
	
	@Override
	public String sqlIsNullFunction(String columnName) {
		return columnName+" is null";
	}

}
