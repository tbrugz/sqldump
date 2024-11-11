package tbrugz.sqldump.resultset;

import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Types;
import java.util.ArrayList;
import java.util.List;

public class ResultSetArrayAdapter extends AbstractNavigationalResultSet {
	
	public static class ResultSetArrayMetaData extends AbstractResultSetMetaData {
		final boolean withIndexColumn;
		final List<String> colNames = new ArrayList<String>();
		final List<Integer> colTypes = new ArrayList<Integer>();
	
		//XXX array type as parameter?
		public ResultSetArrayMetaData(boolean withIndexColumn, String columnName) {
			this.withIndexColumn = withIndexColumn;
			if(withIndexColumn) {
				colNames.add("index");
				colTypes.add(Types.INTEGER);
			}
			colNames.add(columnName);
			colTypes.add(Types.JAVA_OBJECT); //XXX JAVA_OBJECT? STRING?
		}
		
		@Override
		public int getColumnCount() throws SQLException {
			return withIndexColumn?2:1;
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
		public int getColumnType(int column) throws SQLException {
			return colTypes.get(column-1);
		}
		
	}
	
	final Object[] arr;
	final boolean withIndexColumn;
	final String columnName;
	
	int position = -1;
	
	public ResultSetArrayAdapter(Object[] arr, boolean withIndexColumn, String columnName) {
		this.arr = arr;
		this.withIndexColumn = withIndexColumn;
		this.columnName = columnName;
	}

	public ResultSetArrayAdapter(Object[] arr, boolean withIndexColumn) {
		this(arr, withIndexColumn, "object");
	}
	
	@Override
	public ResultSetMetaData getMetaData() throws SQLException {
		return new ResultSetArrayMetaData(withIndexColumn, columnName);
	}
	
	@Override
	public Object getObject(int columnIndex) throws SQLException {
		if(withIndexColumn) {
			if(columnIndex==1) return position;
			if(columnIndex==2) return arr[position];
		}
		if(columnIndex==1) return arr[position];
		throw new IndexOutOfBoundsException("Index out of bounds: "+columnIndex+" [colCount="+(withIndexColumn?2:1)+"]");
	}
	
	@Override
	public String getString(int columnIndex) throws SQLException {
		return String.valueOf(getObject(columnIndex));
	}

	@Override
	public double getDouble(int columnIndex) throws SQLException {
		Object o = getObject(columnIndex);
		if(o instanceof Number) {
			return ((Number) o).doubleValue();
		}
		//return Double.NaN;
		throw new SQLException("not a double: "+o);
	}
	
	@Override
	public int getInt(int columnIndex) throws SQLException {
		Object o = getObject(columnIndex);
		if(o instanceof Number) {
			return ((Number) o).intValue();
		}
		//return Integer.MIN_VALUE;
		throw new SQLException("not an int: "+o);
	}
	
	@Override
	public long getLong(int columnIndex) throws SQLException {
		Object o = getObject(columnIndex);
		if(o instanceof Number) {
			return ((Number) o).longValue();
		}
		//return Long.MIN_VALUE;
		throw new SQLException("not a long: "+o);
	}

	// --- AbstractNavigationalResultSet ---
	
	@Override
	protected void updateCurrentElement() {
	}

	@Override
	protected int getRowCount() {
		return arr.length;
	}

	@Override
	protected int getPosition() {
		return position;
	}

	@Override
	protected void setPosition(int position) {
		this.position = position;
	}

}
