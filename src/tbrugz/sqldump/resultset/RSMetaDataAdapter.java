package tbrugz.sqldump.resultset;

import java.sql.SQLException;
import java.sql.Types;
import java.util.List;

public class RSMetaDataAdapter extends AbstractResultSetMetaData {
	
	final String schema;
	final String table;
	final List<String> colNames;
	
	public RSMetaDataAdapter(String schema, String table, List<String> colNames) {
		this.schema = schema;
		this.table = table;
		this.colNames = colNames;
	}

	@Override
	public int getColumnCount() throws SQLException {
		return colNames.size();
	}

	@Override
	public String getColumnLabel(int column) throws SQLException {
		return colNames.get(column-1);
	}

	@Override
	public String getColumnName(int column) throws SQLException {
		return colNames.get(column-1);
	}

	@Override
	public String getSchemaName(int column) throws SQLException {
		return schema;
	}

	@Override
	public String getTableName(int column) throws SQLException {
		return table;
	}

	@Override
	public int getColumnType(int column) throws SQLException {
		return Types.VARCHAR;
	}

	@Override
	public String getColumnTypeName(int column) throws SQLException {
		return "VARCHAR";
	}

	@Override
	public String getColumnClassName(int column) throws SQLException {
		return String.class.getName();
	}

}
