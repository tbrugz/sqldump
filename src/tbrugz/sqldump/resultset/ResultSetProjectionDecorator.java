package tbrugz.sqldump.resultset;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

public class ResultSetProjectionDecorator extends AbstractResultSetDecorator {

	//private static final Log log = LogFactory.getLog(ResultSetProjectionDecorator.class);

	final RSMetaDataTypedAdapter metadata;
	final Map<Integer, Integer> col2colMap = new HashMap<Integer, Integer>();
	final Map<String, Integer> name2colMap = new HashMap<String, Integer>();
	
	public ResultSetProjectionDecorator(ResultSet rs, List<String> columns) throws SQLException {
		super(rs);
		
		ResultSetMetaData rsmd = super.getMetaData();
		
		int colCount = rsmd.getColumnCount();
		//List<String> finalColNamesToDump = new ArrayList<String>();
		//List<Integer> finalColTypesToDump = new ArrayList<Integer>();
		Map<Integer, String> namesMap = new TreeMap<Integer, String>();
		Map<Integer, Integer> typesMap = new TreeMap<Integer, Integer>();
		for(int i=1;i<=colCount;i++) {
			String colName = rsmd.getColumnName(i);
			int idx = columns.indexOf(colName);
			if(idx==-1) {
				//log.debug("colName '"+colName+"' not found in "+columns);
				String colLabel = rsmd.getColumnLabel(i);
				idx = columns.indexOf(colLabel);
				if(idx==-1) {
					//log.debug("colLabel '"+colLabel+"' not found in "+columns);
					continue;
				}
				colName = colLabel;
			}
			namesMap.put(idx, colName);
			col2colMap.put(idx+1, i);
			name2colMap.put(colName, i);
			
			int colType = rsmd.getColumnType(i);
			typesMap.put(idx, colType);
			
			//finalColNamesToDump.add(colName);
			//finalColTypesToDump.add(colType);
			//if(idx==-1) { continue; }
			//ctArr[idx] = colType;
		}
		//List<Integer> colTypes = Arrays.asList(ctArr);
		/*if(finalColNamesToDump.size() != columns.size()) {
			log.info("filtering ResultSet by (updated) columns: "+finalColNamesToDump+" -- original cols: "+columns);
		}*/
		
		for(int i=0;i<columns.size();i++) {
			String col = namesMap.get(i);
			if(col==null) {
				throw new IllegalArgumentException("column #"+i+" not found: "+columns.get(i));
			}
		}
		//log.info(">> filtering ResultSet by columns: "+namesMap);
		//log.info(">> col2colMap: "+col2colMap+" / name2colMap: "+name2colMap);
		
		List<String> colNames = new ArrayList<String>();
		List<Integer> colTypes = new ArrayList<Integer>();
		for(Integer i: namesMap.keySet()) {
			colNames.add(namesMap.get(i));
			colTypes.add(typesMap.get(i));
		}
		
		metadata = new RSMetaDataTypedAdapter(rsmd.getSchemaName(1), rsmd.getTableName(1), colNames, colTypes);
	}
	
	@Override
	public ResultSetMetaData getMetaData() throws SQLException {
		return metadata;
	}
	
	int colIndex2Col(int columnIndex) {
		return col2colMap.get(columnIndex);
	}

	int colLabel2Col(String colName) {
		return name2colMap.get(colName);
	}
	
	// ----------------------------------------- //
	
	@Override
	public String getString(int columnIndex) throws SQLException {
		return super.getString(colIndex2Col(columnIndex));
	}

	@Override
	public boolean getBoolean(int columnIndex) throws SQLException {
		return super.getBoolean(colIndex2Col(columnIndex));
	}

	@Override
	public byte getByte(int columnIndex) throws SQLException {
		return super.getByte(colIndex2Col(columnIndex));
	}

	@Override
	public short getShort(int columnIndex) throws SQLException {
		return super.getShort(colIndex2Col(columnIndex));
	}

	@Override
	public int getInt(int columnIndex) throws SQLException {
		return super.getInt(colIndex2Col(columnIndex));
	}

	@Override
	public long getLong(int columnIndex) throws SQLException {
		return super.getLong(colIndex2Col(columnIndex));
	}

	@Override
	public float getFloat(int columnIndex) throws SQLException {
		return super.getFloat(colIndex2Col(columnIndex));
	}

	@Override
	public double getDouble(int columnIndex) throws SQLException {
		return super.getDouble(colIndex2Col(columnIndex));
	}

	/*
	public BigDecimal getBigDecimal(int columnIndex, int scale)
			throws SQLException {
		return rs.getBigDecimal(columnIndex, scale);
	}

	public byte[] getBytes(int columnIndex) throws SQLException {
		return rs.getBytes(columnIndex);
	}

	public Date getDate(int columnIndex) throws SQLException {
		return rs.getDate(columnIndex);
	}

	public Time getTime(int columnIndex) throws SQLException {
		return rs.getTime(columnIndex);
	}

	public Timestamp getTimestamp(int columnIndex) throws SQLException {
		return rs.getTimestamp(columnIndex);
	}

	public InputStream getAsciiStream(int columnIndex) throws SQLException {
		return rs.getAsciiStream(columnIndex);
	}

	public InputStream getUnicodeStream(int columnIndex) throws SQLException {
		return rs.getUnicodeStream(columnIndex);
	}

	public InputStream getBinaryStream(int columnIndex) throws SQLException {
		return rs.getBinaryStream(columnIndex);
	}
	*/

	@Override
	public String getString(String columnLabel) throws SQLException {
		return super.getString(colLabel2Col(columnLabel));
	}

	@Override
	public boolean getBoolean(String columnLabel) throws SQLException {
		return super.getBoolean(colLabel2Col(columnLabel));
	}

	@Override
	public byte getByte(String columnLabel) throws SQLException {
		return super.getByte(colLabel2Col(columnLabel));
	}

	@Override
	public short getShort(String columnLabel) throws SQLException {
		return super.getShort(colLabel2Col(columnLabel));
	}

	@Override
	public int getInt(String columnLabel) throws SQLException {
		return super.getInt(colLabel2Col(columnLabel));
	}
	
	@Override
	public long getLong(String columnLabel) throws SQLException {
		return super.getLong(colLabel2Col(columnLabel));
	}

	@Override
	public float getFloat(String columnLabel) throws SQLException {
		return super.getFloat(colLabel2Col(columnLabel));
	}

	@Override
	public double getDouble(String columnLabel) throws SQLException {
		return super.getDouble(colLabel2Col(columnLabel));
	}

	/*
	public BigDecimal getBigDecimal(String columnLabel, int scale)
			throws SQLException {
		return rs.getBigDecimal(columnLabel, scale);
	}

	public byte[] getBytes(String columnLabel) throws SQLException {
		return rs.getBytes(columnLabel);
	}

	public Date getDate(String columnLabel) throws SQLException {
		return rs.getDate(columnLabel);
	}

	public Time getTime(String columnLabel) throws SQLException {
		return rs.getTime(columnLabel);
	}

	public Timestamp getTimestamp(String columnLabel) throws SQLException {
		return rs.getTimestamp(columnLabel);
	}

	public InputStream getAsciiStream(String columnLabel) throws SQLException {
		return rs.getAsciiStream(columnLabel);
	}

	public InputStream getUnicodeStream(String columnLabel) throws SQLException {
		return rs.getUnicodeStream(columnLabel);
	}

	public InputStream getBinaryStream(String columnLabel) throws SQLException {
		return rs.getBinaryStream(columnLabel);
	}
	*/

	@Override
	public Object getObject(int columnIndex) throws SQLException {
		return super.getObject(colIndex2Col(columnIndex));
	}

	@Override
	public Object getObject(String columnLabel) throws SQLException {
		return super.getObject(colLabel2Col(columnLabel));
	}

	
}
