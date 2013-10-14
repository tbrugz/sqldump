package tbrugz.sqldump.pivot;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import tbrugz.sqldump.jdbc.AbstractStatementDecorator;
import tbrugz.sqldump.resultset.pivot.PivotResultSet;

/**
 * Possible sql-pivot syntaxes:
 * <code>select col1, col2, col3, col4 m1, m2 from (...) /-* pivot col3 asc, col4 desc measures m1, m2 *-/</code>
 * <code>select col1, col2, col3, col4 m1, m2 from (...) /-* pivot col3 asc, col4 desc nonpivot col1, col2 *-/</code>
 */
public class PivotStatement<S extends Statement> extends AbstractStatementDecorator<S> {
	
	ResultSet rs;
	
	public PivotStatement(S statement) {
		super(statement);
	}

	//XXX
	public ResultSet executeQuery(String sql) throws SQLException {
		if(sql==null) {
			throw new IllegalArgumentException("query must not be null");
		}
		PivotQueryParser parser = new PivotQueryParser(sql);
		if(parser.colsToPivot!=null) {
			return new PivotResultSet(statement.executeQuery(sql), parser.colsNotToPivot, parser.colsToPivot, true, parser.flags);
		}
		return statement.executeQuery(sql);
	}

	//TODO ??
	public boolean execute(String sql) throws SQLException {
		if(sql==null) {
			throw new IllegalArgumentException("query must not be null");
		}
		PivotQueryParser parser = new PivotQueryParser(sql);
		if(parser.colsToPivot!=null) {
			rs = new PivotResultSet(statement.executeQuery(sql), parser.colsNotToPivot, parser.colsToPivot, true, parser.flags);
			return true;
		}
		return statement.execute(sql);
	}

	//TODO ??
	public ResultSet getResultSet() throws SQLException {
		if(rs!=null) {
			ResultSet rstmp = rs;
			rs = null;
			return rstmp;
		}
		return statement.getResultSet();
	}

}
