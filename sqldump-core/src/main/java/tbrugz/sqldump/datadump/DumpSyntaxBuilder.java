package tbrugz.sqldump.datadump;

import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.List;
import java.util.Properties;

public interface DumpSyntaxBuilder {

	//public String getSyntaxId();

	//public String getMimeType();
	
	//public String getDefaultFileExtension();
	
	public void procProperties(Properties prop);
	
	public DumpSyntaxInt build(String schemaName, String tableName, List<String> pkCols, ResultSetMetaData md) throws SQLException;
	
}
