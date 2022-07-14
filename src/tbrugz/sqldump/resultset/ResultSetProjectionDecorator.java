package tbrugz.sqldump.resultset;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Types;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import tbrugz.sqldump.util.SQLUtils;

public class ResultSetProjectionDecorator extends AbstractResultSetDecorator {

	private static final Log log = LogFactory.getLog(ResultSetProjectionDecorator.class);

	protected final RSMetaDataTypedAdapter metadata;
	protected final Map<Integer, Integer> col2colMap = new LinkedHashMap<Integer, Integer>();
	protected final Map<String, Integer> name2colMap = new LinkedHashMap<String, Integer>();
	
	public ResultSetProjectionDecorator(ResultSet rs, List<String> columns) throws SQLException {
		this(rs, columns, null, false);
	}

	public ResultSetProjectionDecorator(ResultSet rs, List<String> columns, boolean ignoreInvalidColumns) throws SQLException {
		this(rs, columns, null, ignoreInvalidColumns);
	}

	public ResultSetProjectionDecorator(ResultSet rs, List<String> columns, List<String> aliases, boolean ignoreInvalidColumns) throws SQLException {
		super(rs);
		
		if(aliases!=null && aliases.size()!=columns.size()) {
			throw new IllegalArgumentException("aliases list must have same size as columns list [cols="+columns+";aliases="+aliases+"]");
		}
		
		ResultSetMetaData rsmd = super.getMetaData();
		
		Map<Integer, String> namesMap = new TreeMap<Integer, String>();
		Map<Integer, Integer> typesMap = new TreeMap<Integer, Integer>();
		List<String> colsNotFound = new ArrayList<String>();
		if(!ignoreInvalidColumns) {
			int colCount = rsmd.getColumnCount();
			for(int i=1;i<=colCount;i++) {
				String colName = rsmd.getColumnName(i);
				int idx = columns.indexOf(colName);
				if(idx==-1) {
					//log.debug("colName '"+colName+"' not found in "+columns);
					String colLabel = rsmd.getColumnLabel(i);
					idx = columns.indexOf(colLabel);
					if(idx==-1) {
						//log.debug("colLabel '"+colLabel+"' not found in "+columns);
						colsNotFound.add(colLabel);
						continue;
					}
					colName = colLabel;
				}
				namesMap.put(idx, colName);
				col2colMap.put(idx+1, i);
				name2colMap.put(colName, i);
				
				int colType = rsmd.getColumnType(i);
				typesMap.put(idx, colType);
			}
		}
		else {
			List<String> rsColNames = SQLUtils.getColumnNamesAsList(rsmd);
			for(int i=0;i<columns.size();i++) {
				String colName = columns.get(i);
				int idx = rsColNames.indexOf(colName);
				int colType = Types.VARCHAR;
				if(idx==-1) {
					colsNotFound.add(colName);
					//continue;
				}
				else {
					colType = rsmd.getColumnType(idx+1);
				}
				
				col2colMap.put(i+1, idx+1);
				name2colMap.put(colName, idx+1);
				namesMap.put(i, colName);
				typesMap.put(i, colType);
			}
		}
		
		if(colsNotFound.size()>0) {
			log.info("colsNotFound: "+colsNotFound+" ; namesMap.size()="+namesMap.size()+" ; colsNotFound.size()="+colsNotFound.size());
		}
		/*for(int i=namesMap.size();i<colsNotFound.size();i++) {
			String colName = colsNotFound.get(i);
			
			namesMap.put(i, colName);
			col2colMap.put(i+1, i);
			name2colMap.put(colName, i);
			//int colType = rsmd.getColumnType(i);
			typesMap.put(i, null);
		}*/
		//List<Integer> colTypes = Arrays.asList(ctArr);
		/*if(finalColNamesToDump.size() != columns.size()) {
			log.info("filtering ResultSet by (updated) columns: "+finalColNamesToDump+" -- original cols: "+columns);
		}*/
		
		if(!ignoreInvalidColumns) {
			for(int i=0;i<columns.size();i++) {
				String col = namesMap.get(i);
				if(col==null) {
					throw new IllegalArgumentException("column #"+i+" not found: "+columns.get(i));
				}
			}
		}
		//log.info("> namesMap: "+namesMap+" / typesMap: "+typesMap);
		//log.info(">> col2colMap: "+col2colMap+" / name2colMap: "+name2colMap);
		
		List<String> colNames = new ArrayList<String>();
		List<Integer> colTypes = new ArrayList<Integer>();
		for(Integer i: namesMap.keySet()) {
			colNames.add(namesMap.get(i));
			colTypes.add(typesMap.get(i));
		}
		
		metadata = new RSMetaDataTypedAdapter(rsmd.getSchemaName(1), rsmd.getTableName(1), aliases!=null?aliases:colNames, colTypes);
	}
	
	@Override
	public ResultSetMetaData getMetaData() throws SQLException {
		return metadata;
	}
	
	int colIndex2Col(int columnIndex) {
		Integer ret = col2colMap.get(columnIndex);
		return ret==null?0:ret; 
	}

	int colLabel2Col(String colName) {
		Integer ret = name2colMap.get(colName);
		return ret==null?0:ret; 
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
