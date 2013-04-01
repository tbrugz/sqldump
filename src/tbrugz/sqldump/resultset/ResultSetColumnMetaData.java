package tbrugz.sqldump.resultset;

import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import tbrugz.sqldump.SQLUtils;

public class ResultSetColumnMetaData {

	final int numCol;
	final List<String> colNames = new ArrayList<String>();
	final List<Class<?>> colTypes = new ArrayList<Class<?>>();
	final ResultSetMetaData md;
	
	public ResultSetColumnMetaData(ResultSetMetaData md) {
		try {
			this.md = md;
			this.numCol = md.getColumnCount();
			
			for(int i=0;i<numCol;i++) {
				colNames.add(md.getColumnName(i+1));
			}
			for(int i=0;i<numCol;i++) {
				colTypes.add(SQLUtils.getClassFromSqlType(md.getColumnType(i+1), md.getPrecision(i+1), md.getScale(i+1)));
			}
		}
		catch(SQLException e) {
			throw new RuntimeException(e);
		}
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result
				+ ((colNames == null) ? 0 : colNames.hashCode());
		result = prime * result
				+ ((colTypes == null) ? 0 : colTypes.hashCode());
		result = prime * result + numCol;
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		ResultSetColumnMetaData other = (ResultSetColumnMetaData) obj;
		if (colNames == null) {
			if (other.colNames != null)
				return false;
		} else if (!colNames.equals(other.colNames))
			return false;
		if (colTypes == null) {
			if (other.colTypes != null)
				return false;
		} else if (!colTypes.equals(other.colTypes))
			return false;
		if (numCol != other.numCol)
			return false;
		return true;
	}
	
	/*@Override
	public boolean equals(Object obj) {
		if(obj instanceof ResultSetColumnMetaData) {
			ResultSetColumnMetaData other = (ResultSetColumnMetaData) obj;
		}
		return false;
	}*/
	
}
