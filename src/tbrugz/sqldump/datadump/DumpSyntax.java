package tbrugz.sqldump.datadump;

import java.io.Writer;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

public abstract class DumpSyntax {
	
	static final Class[] arr = {InsertIntoDataDump.class, CSVDataDump.class, XMLDataDump.class, JSONDataDump.class};
	
	public static List<Class> getSyntaxes() {
		return Arrays.asList(arr);
	}
	
	public abstract void procProperties(Properties prop);
	
	public abstract String getSyntaxId();
	
	public abstract void initDump(String tableName, ResultSetMetaData md) throws Exception;
	
	public abstract void dumpHeader(Writer fos) throws Exception;

	public abstract void dumpRow(ResultSet rs, int count, Writer fos) throws Exception;

	public abstract void dumpFooter(Writer fos) throws Exception;
}
