package tbrugz.sqldump.pivot;

import java.io.InputStream;
import java.io.Reader;
import java.math.BigDecimal;
import java.net.URL;
import java.sql.Array;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.Date;
import java.sql.NClob;
import java.sql.ParameterMetaData;
import java.sql.PreparedStatement;
import java.sql.Ref;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.RowId;
import java.sql.SQLException;
import java.sql.SQLXML;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.Calendar;

import tbrugz.sqldump.resultset.pivot.PivotResultSet;

public class PivotPreparedStatement extends PivotStatement<PreparedStatement> implements PreparedStatement {

	//final PreparedStatement statement;
	final String sql;

	public PivotPreparedStatement(PreparedStatement statement, String sql) {
		super(statement);
		this.sql = sql;
		//this.statement = statement;
	}
	
	//XXX
	public ResultSet executeQuery() throws SQLException {
		PivotQueryParser parser = new PivotQueryParser(sql);
		return new PivotResultSet(statement.executeQuery(), parser.colsNotToPivot, parser.colsToPivot, true, parser.flags);
		//return statement.executeQuery();
	}

	public int executeUpdate() throws SQLException {
		return statement.executeUpdate();
	}

	public void setNull(int parameterIndex, int sqlType) throws SQLException {
		statement.setNull(parameterIndex, sqlType);
	}

	public void setBoolean(int parameterIndex, boolean x) throws SQLException {
		statement.setBoolean(parameterIndex, x);
	}

	public void setMaxFieldSize(int max) throws SQLException {
		statement.setMaxFieldSize(max);
	}

	public void setByte(int parameterIndex, byte x) throws SQLException {
		statement.setByte(parameterIndex, x);
	}

	public void setShort(int parameterIndex, short x) throws SQLException {
		statement.setShort(parameterIndex, x);
	}

	public void setInt(int parameterIndex, int x) throws SQLException {
		statement.setInt(parameterIndex, x);
	}

	public void setMaxRows(int max) throws SQLException {
		statement.setMaxRows(max);
	}

	public void setLong(int parameterIndex, long x) throws SQLException {
		statement.setLong(parameterIndex, x);
	}

	public void setEscapeProcessing(boolean enable) throws SQLException {
		statement.setEscapeProcessing(enable);
	}

	public void setFloat(int parameterIndex, float x) throws SQLException {
		statement.setFloat(parameterIndex, x);
	}

	public void setDouble(int parameterIndex, double x) throws SQLException {
		statement.setDouble(parameterIndex, x);
	}

	public void setQueryTimeout(int seconds) throws SQLException {
		statement.setQueryTimeout(seconds);
	}

	public void setBigDecimal(int parameterIndex, BigDecimal x)
			throws SQLException {
		statement.setBigDecimal(parameterIndex, x);
	}

	public void setString(int parameterIndex, String x) throws SQLException {
		statement.setString(parameterIndex, x);
	}

	public void setBytes(int parameterIndex, byte[] x) throws SQLException {
		statement.setBytes(parameterIndex, x);
	}

	public void setDate(int parameterIndex, Date x) throws SQLException {
		statement.setDate(parameterIndex, x);
	}

	public void setCursorName(String name) throws SQLException {
		statement.setCursorName(name);
	}

	public void setTime(int parameterIndex, Time x) throws SQLException {
		statement.setTime(parameterIndex, x);
	}

	public void setTimestamp(int parameterIndex, Timestamp x)
			throws SQLException {
		statement.setTimestamp(parameterIndex, x);
	}

	public void setAsciiStream(int parameterIndex, InputStream x, int length)
			throws SQLException {
		statement.setAsciiStream(parameterIndex, x, length);
	}

	@SuppressWarnings("deprecation")
	public void setUnicodeStream(int parameterIndex, InputStream x, int length)
			throws SQLException {
		statement.setUnicodeStream(parameterIndex, x, length);
	}

	public void setBinaryStream(int parameterIndex, InputStream x, int length)
			throws SQLException {
		statement.setBinaryStream(parameterIndex, x, length);
	}

	public void clearParameters() throws SQLException {
		statement.clearParameters();
	}

	public void setObject(int parameterIndex, Object x, int targetSqlType)
			throws SQLException {
		statement.setObject(parameterIndex, x, targetSqlType);
	}

	public void setObject(int parameterIndex, Object x) throws SQLException {
		statement.setObject(parameterIndex, x);
	}

	public void addBatch(String sql) throws SQLException {
		statement.addBatch(sql);
	}

	public void clearBatch() throws SQLException {
		statement.clearBatch();
	}

	public boolean execute() throws SQLException {
		return statement.execute();
	}

	public void addBatch() throws SQLException {
		statement.addBatch();
	}

	public void setCharacterStream(int parameterIndex, Reader reader, int length)
			throws SQLException {
		statement.setCharacterStream(parameterIndex, reader, length);
	}

	public void setRef(int parameterIndex, Ref x) throws SQLException {
		statement.setRef(parameterIndex, x);
	}

	public void setBlob(int parameterIndex, Blob x) throws SQLException {
		statement.setBlob(parameterIndex, x);
	}

	public void setClob(int parameterIndex, Clob x) throws SQLException {
		statement.setClob(parameterIndex, x);
	}

	public boolean getMoreResults(int current) throws SQLException {
		return statement.getMoreResults(current);
	}

	public void setArray(int parameterIndex, Array x) throws SQLException {
		statement.setArray(parameterIndex, x);
	}

	//XXX ??
	public ResultSetMetaData getMetaData() throws SQLException {
		return statement.getMetaData();
	}

	public void setDate(int parameterIndex, Date x, Calendar cal)
			throws SQLException {
		statement.setDate(parameterIndex, x, cal);
	}

	public void setTime(int parameterIndex, Time x, Calendar cal)
			throws SQLException {
		statement.setTime(parameterIndex, x, cal);
	}

	public void setTimestamp(int parameterIndex, Timestamp x, Calendar cal)
			throws SQLException {
		statement.setTimestamp(parameterIndex, x, cal);
	}

	public void setNull(int parameterIndex, int sqlType, String typeName)
			throws SQLException {
		statement.setNull(parameterIndex, sqlType, typeName);
	}

	public void setURL(int parameterIndex, URL x) throws SQLException {
		statement.setURL(parameterIndex, x);
	}

	public ParameterMetaData getParameterMetaData() throws SQLException {
		return statement.getParameterMetaData();
	}

	public void setRowId(int parameterIndex, RowId x) throws SQLException {
		statement.setRowId(parameterIndex, x);
	}

	public void setNString(int parameterIndex, String value)
			throws SQLException {
		statement.setNString(parameterIndex, value);
	}

	public void setNCharacterStream(int parameterIndex, Reader value,
			long length) throws SQLException {
		statement.setNCharacterStream(parameterIndex, value, length);
	}

	public void setNClob(int parameterIndex, NClob value) throws SQLException {
		statement.setNClob(parameterIndex, value);
	}

	public void setClob(int parameterIndex, Reader reader, long length)
			throws SQLException {
		statement.setClob(parameterIndex, reader, length);
	}

	public void setBlob(int parameterIndex, InputStream inputStream, long length)
			throws SQLException {
		statement.setBlob(parameterIndex, inputStream, length);
	}

	public void setNClob(int parameterIndex, Reader reader, long length)
			throws SQLException {
		statement.setNClob(parameterIndex, reader, length);
	}

	public void setSQLXML(int parameterIndex, SQLXML xmlObject)
			throws SQLException {
		statement.setSQLXML(parameterIndex, xmlObject);
	}

	public void setObject(int parameterIndex, Object x, int targetSqlType,
			int scaleOrLength) throws SQLException {
		statement.setObject(parameterIndex, x, targetSqlType, scaleOrLength);
	}

	public void setAsciiStream(int parameterIndex, InputStream x, long length)
			throws SQLException {
		statement.setAsciiStream(parameterIndex, x, length);
	}

	public void setBinaryStream(int parameterIndex, InputStream x, long length)
			throws SQLException {
		statement.setBinaryStream(parameterIndex, x, length);
	}

	public void setCharacterStream(int parameterIndex, Reader reader,
			long length) throws SQLException {
		statement.setCharacterStream(parameterIndex, reader, length);
	}

	public void setAsciiStream(int parameterIndex, InputStream x)
			throws SQLException {
		statement.setAsciiStream(parameterIndex, x);
	}

	public void setBinaryStream(int parameterIndex, InputStream x)
			throws SQLException {
		statement.setBinaryStream(parameterIndex, x);
	}

	public void setCharacterStream(int parameterIndex, Reader reader)
			throws SQLException {
		statement.setCharacterStream(parameterIndex, reader);
	}

	public void setNCharacterStream(int parameterIndex, Reader value)
			throws SQLException {
		statement.setNCharacterStream(parameterIndex, value);
	}

	public void setClob(int parameterIndex, Reader reader) throws SQLException {
		statement.setClob(parameterIndex, reader);
	}

	public void setBlob(int parameterIndex, InputStream inputStream)
			throws SQLException {
		statement.setBlob(parameterIndex, inputStream);
	}

	public void setNClob(int parameterIndex, Reader reader) throws SQLException {
		statement.setNClob(parameterIndex, reader);
	}
	
}
