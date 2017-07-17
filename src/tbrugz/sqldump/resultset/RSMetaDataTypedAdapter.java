package tbrugz.sqldump.resultset;

import java.math.BigInteger;
import java.sql.Date;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.sql.Types;
import java.util.List;

public class RSMetaDataTypedAdapter extends RSMetaDataAdapter {

	final List<Integer> colTypes;

	public RSMetaDataTypedAdapter(String schema, String table, List<String> colNames, List<Integer> colTypes) {
		super(schema, table, colNames);
		this.colTypes = colTypes;
	}

	@Override
	public int getColumnType(int column) throws SQLException {
		return colTypes.get(column-1);
	}

	@Override
	public String getColumnTypeName(int column) throws SQLException {
		return getTypeName(colTypes.get(column-1));
	}

	@Override
	public String getColumnClassName(int column) throws SQLException {
		return getClassName(colTypes.get(column-1));
	}
	
	static String getTypeName(int type) {
		switch (type) {
		case Types.TINYINT:
		case Types.SMALLINT:
		case Types.INTEGER:
			return "INTEGER";
			
		case Types.BIGINT:
			return "BIGINT";
			
		case Types.DECIMAL:
		case Types.DOUBLE:
		case Types.FLOAT:
		case Types.NUMERIC:
			return "DECIMAL";
			
		case Types.DATE:
			return "DATE";
		case Types.TIMESTAMP:
			return "TIMESTAMP";
			
		case Types.CHAR:
			return "CHAR";
		case Types.VARCHAR:
			return "VARCHAR";
			
		default:
			return "VARCHAR";
		}
	}

	static String getClassName(int type) {
		switch (type) {
		case Types.TINYINT:
		case Types.SMALLINT:
		case Types.INTEGER:
			return Integer.class.getName();
			
		case Types.BIGINT:
			return BigInteger.class.getName();
			
		case Types.DECIMAL:
		case Types.DOUBLE:
		case Types.FLOAT:
		case Types.NUMERIC:
			return Double.class.getName();
			
		case Types.DATE:
			return Date.class.getName();
		case Types.TIMESTAMP:
			return Timestamp.class.getName();
			
		case Types.CHAR:
		case Types.VARCHAR:
			return String.class.getName();
			
		default:
			return Object.class.getName();
		}
	}
	
}
