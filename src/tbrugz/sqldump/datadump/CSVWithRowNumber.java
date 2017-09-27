package tbrugz.sqldump.datadump;

import java.io.IOException;
import java.io.Writer;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import tbrugz.sqldump.util.SQLUtils;

public class CSVWithRowNumber extends CSVDataDump implements Cloneable {

	//XXX: option to define line number column label
	static final String LINE_NUMBER_COL_LABEL = "LineNumber";
	
	List<Class<?>> lsColTypes4ResultSet = new ArrayList<Class<?>>();
	int numCol4RS = 0;
	
	@Override
	public void initDump(String schema, String tableName, List<String> pkCols,
			ResultSetMetaData md) throws SQLException {
		super.initDump(schema, tableName, pkCols, md);
		lsColNames.add(0, LINE_NUMBER_COL_LABEL);
		
		lsColTypes4ResultSet.addAll(lsColTypes);
		lsColTypes.add(0, Integer.class);
		
		numCol4RS = numCol;
		numCol++;
	}
	
	@Override
	public void dumpRow(ResultSet rs, long count, Writer fos)
			throws IOException, SQLException {
		List<Object> vals = SQLUtils.getRowObjectListFromRS(rs, lsColTypes4ResultSet, numCol4RS);
		vals.add(0, Long.valueOf(count+1));
		dumpValues(vals, count, fos);
	}
	
	//XXX: option to define footer/tralier pattern
	@Override
	public void dumpFooter(long count, boolean hasMoreRows, Writer fos) throws IOException {
		//out(String.valueOf(count+1), fos, recordDelimiter);
	}
	
	@Override
	public String getSyntaxId() {
		return "rncsv";
	}
	
	@Override
	public String getDefaultFileExtension() {
		return "rn.csv";
	}
}
