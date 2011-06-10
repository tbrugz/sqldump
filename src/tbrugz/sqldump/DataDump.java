package tbrugz.sqldump;

import java.io.FileWriter;
import java.io.IOException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.Statement;
import java.util.List;
import java.util.Properties;

import org.apache.log4j.Logger;

public class DataDump {

	static final String PROP_DATADUMP_FILEPATTERN = "sqldump.datadump.filepattern";
	static final String PROP_DATADUMP_TABLENAMEHEADER = "sqldump.datadump.tablenameheader";
	static final String PROP_DATADUMP_RECORDDELIMITER = "sqldump.datadump.recorddelimiter";
	static final String PROP_DATADUMP_COLUMNDELIMITER = "sqldump.datadump.columndelimiter";
	
	static final String DELIM_RECORD_DEFAULT = "\n";
	static final String DELIM_COLUMN_DEFAULT = ";";
	
	static final String FILENAME_PATTERN_TABLENAME = "\\$\\{tablename\\}";
	
	static Logger log = Logger.getLogger(SQLDump.class);
	
	void dumpData(Connection conn, List<String> tableNamesForDataDump, Properties prop) throws Exception {
		log.info("data dumping...");
		
		String recordDelimiter = prop.getProperty(PROP_DATADUMP_RECORDDELIMITER, DELIM_RECORD_DEFAULT);
		String columnDelimiter = prop.getProperty(PROP_DATADUMP_COLUMNDELIMITER, DELIM_COLUMN_DEFAULT);
		
		for(String table: tableNamesForDataDump) {
			String filename = prop.getProperty(PROP_DATADUMP_FILEPATTERN);
			filename = filename.replaceAll(FILENAME_PATTERN_TABLENAME, table);
			FileWriter fos = new FileWriter(filename);
			
			Statement st = conn.createStatement();
			log.info("dumping data from table: "+table);
			ResultSet rs = st.executeQuery("select * from \""+table+"\"");
			if("true".equals(prop.getProperty(PROP_DATADUMP_TABLENAMEHEADER, "false"))) {
				out("[table "+table+"]", fos, recordDelimiter);
			}
			ResultSetMetaData md = rs.getMetaData();
			int numCol = md.getColumnCount();
			while(rs.next()) {
				out(SQLUtils.getRowFromRS(rs, numCol, table, columnDelimiter), fos, recordDelimiter);
			}
			rs.close();
			
			fos.close();
		}
		
	}
	
	void out(String s, FileWriter fos, String recordDelimiter) throws IOException {
		fos.write(s+recordDelimiter);
	}
	
}
