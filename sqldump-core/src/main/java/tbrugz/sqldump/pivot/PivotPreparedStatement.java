package tbrugz.sqldump.pivot;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;

import tbrugz.sqldump.jdbc.AbstractPreparedStatementDecorator;
import tbrugz.sqldump.resultset.pivot.PivotResultSet;

public class PivotPreparedStatement extends AbstractPreparedStatementDecorator implements PreparedStatement {

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
		if(parser.colsToPivot!=null) {
			return new PivotResultSet(statement.executeQuery(), parser.colsNotToPivot, parser.colsToPivot, true, parser.flags);
		}
		return statement.executeQuery();
	}

	//XXX ??
	public ResultSetMetaData getMetaData() throws SQLException {
		return statement.getMetaData();
	}

}
