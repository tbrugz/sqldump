package tbrugz.sqldump.dbmsfeatures;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Types;

import org.h2.api.AggregateFunction;

public class H2CountAggregate implements AggregateFunction {

	int count = 0;
	
	@Override
	public void init(Connection conn) throws SQLException {
		count = 0;
	}

	@Override
	public int getType(int[] inputTypes) throws SQLException {
		return Types.INTEGER;
	}

	@Override
	public void add(Object value) throws SQLException {
		count++;
	}

	@Override
	public Object getResult() throws SQLException {
		return count;
	}

}
