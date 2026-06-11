package tbrugz.sqldump.resultset;

import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class ResultSetArrayAdapter extends AbstractNavigationalResultSet {
	
	private static final Log log = LogFactory.getLog(ResultSetArrayAdapter.class);
	
	public static class ResultSetArrayMetaData extends AbstractResultSetMetaData {
		final boolean withIndexColumn;
		final List<String> colNames = new ArrayList<String>();
		final List<Integer> colTypes = new ArrayList<Integer>();
	
		public ResultSetArrayMetaData(boolean withIndexColumn, String columnName, Class<?> columnType) {
			this.withIndexColumn = withIndexColumn;
			if(withIndexColumn) {
				colNames.add("index");
				colTypes.add(Types.INTEGER);
			}
			colNames.add(columnName);
			colTypes.add(getType(columnType));
		}

		int getType(Class<?> type) {
			if(type == Boolean.TYPE) { return Types.BOOLEAN; }
			if(type == Boolean.class) { return Types.BOOLEAN; }
			//XXX add more types?
			//XXX JAVA_OBJECT? STRING?
			return Types.JAVA_OBJECT;
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
	final Class<?> arrType;
	final boolean withIndexColumn;
	final String columnName;
	
	int position = -1;
	
	public ResultSetArrayAdapter(Object[] arr, boolean withIndexColumn, String columnName) {
		this.arr = arr;
		this.withIndexColumn = withIndexColumn;
		this.columnName = columnName;
		this.arrType = getArrayType(arr);
		//log.info("ResultSetArrayAdapter:: arr["+arrType.getSimpleName()+"]: "+Arrays.asList(arr));
	}
	
	static Class<?> getArrayType(Object[] arr) {
		if(arr.length>0) {
			if(arr[0]==null) {
				//log.warn("ResultSetArrayAdapter:: arr[0]==null");
				return arr.getClass().getComponentType();
			}
			else {
				//log.debug("ResultSetArrayAdapter:: arr[0]!=null");
				return arr[0].getClass();
			}
		}
		else {
			//log.debug("ResultSetArrayAdapter:: arr.length==0");
			return arr.getClass().getComponentType();
		}
	}

	public ResultSetArrayAdapter(Object[] arr, boolean withIndexColumn) {
		this(arr, withIndexColumn, "object");
	}
	
	@Override
	public ResultSetMetaData getMetaData() throws SQLException {
		return new ResultSetArrayMetaData(withIndexColumn, columnName, arrType);
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
	
	@Override
	public boolean getBoolean(int columnIndex) throws SQLException {
		Object o = getObject(columnIndex);
		if(o instanceof Boolean) {
			return ((Boolean) o).booleanValue();
		}
		//return false;
		throw new SQLException("not a boolean: "+o);
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
