package tbrugz.sqldump.resultset.pivot;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import tbrugz.sqldump.resultset.ResultSetDecoratorFactory;
import tbrugz.sqldump.util.Utils;

public class PivotRSFactory implements ResultSetDecoratorFactory {

	final static Log log = LogFactory.getLog(PivotRSFactory.class);

	static final String PARAM_COLS_TO_PIVOT = "colstopivot";
	static final String PARAM_COLS_NOT_TO_PIVOT = "colsnottopivot";

	List<String> colsToPivot;
	List<String> colsNotToPivot;
	
	@Override
	public void set(String key, String value) {
		if(PARAM_COLS_TO_PIVOT.equals(key)) {
			colsToPivot = Utils.splitStringWithTrim(value, ",");
		}
		else if(PARAM_COLS_NOT_TO_PIVOT.equals(key)) {
			colsNotToPivot = Utils.splitStringWithTrim(value, ",");
		}
		else {
			log.warn("unknown parameter: "+key);
		}
	}

	@Override
	public ResultSet getDecoratorOf(ResultSet arg) {
		try {
			return new PivotResultSet(arg, colsNotToPivot, colsToPivot, true);
		} catch (SQLException e) {
			throw new RuntimeException(e);
		}
	}
}
