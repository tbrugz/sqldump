package tbrugz.sqldump;

import java.io.IOException;
import java.io.PrintWriter;
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
	static final String PROP_DATADUMP_COLUMNNAMESHEADER = "sqldump.datadump.columnnamesheader";
	static final String PROP_DATADUMP_RECORDDELIMITER = "sqldump.datadump.recorddelimiter";
	static final String PROP_DATADUMP_COLUMNDELIMITER = "sqldump.datadump.columndelimiter";
	static final String PROP_DATADUMP_CHARSET = "sqldump.datadump.charset";

	static final String DELIM_RECORD_DEFAULT = "\n";
	static final String DELIM_COLUMN_DEFAULT = ";";
	static final String CHARSET_DEFAULT = "UTF-8";
	
	static final String FILENAME_PATTERN_TABLENAME = "\\$\\{tablename\\}";
	
	static Logger log = Logger.getLogger(SQLDump.class);
	
	void dumpData(Connection conn, List<String> tableNamesForDataDump, Properties prop) throws Exception {
		log.info("data dumping...");
		
		String recordDelimiter = prop.getProperty(PROP_DATADUMP_RECORDDELIMITER, DELIM_RECORD_DEFAULT);
		String columnDelimiter = prop.getProperty(PROP_DATADUMP_COLUMNDELIMITER, DELIM_COLUMN_DEFAULT);
		String charset = prop.getProperty(PROP_DATADUMP_CHARSET, CHARSET_DEFAULT);
		boolean doTableNameHeaderDump = "true".equals(prop.getProperty(PROP_DATADUMP_TABLENAMEHEADER, "false"));
		boolean doColumnNamesHeaderDump = "true".equals(prop.getProperty(PROP_DATADUMP_COLUMNNAMESHEADER, "false"));
		
		/*
		 * charset: http://download.oracle.com/javase/6/docs/api/java/nio/charset/Charset.html
		 *
		 * US-ASCII 	Seven-bit ASCII, a.k.a. ISO646-US, a.k.a. the Basic Latin block of the Unicode character set
		 * ISO-8859-1   	ISO Latin Alphabet No. 1, a.k.a. ISO-LATIN-1
		 * UTF-8 	Eight-bit UCS Transformation Format
		 * UTF-16BE 	Sixteen-bit UCS Transformation Format, big-endian byte order
		 * UTF-16LE 	Sixteen-bit UCS Transformation Format, little-endian byte order
		 * UTF-16 	Sixteen-bit UCS Transformation Format, byte order identified by an optional byte-order mark
		 *
		 * XXX: use java.nio.charset.Charset.availableCharsets() ?
		 *  
		 */
		
		for(String table: tableNamesForDataDump) {
			String filename = prop.getProperty(PROP_DATADUMP_FILEPATTERN);
			filename = filename.replaceAll(FILENAME_PATTERN_TABLENAME, table);
			PrintWriter fos = new PrintWriter(filename, charset);
			
			log.info("dumping data from table: "+table);
			Statement st = conn.createStatement();
			ResultSet rs = st.executeQuery("select * from \""+table+"\"");
			ResultSetMetaData md = rs.getMetaData();
			int numCol = md.getColumnCount();

			//headers
			if(doTableNameHeaderDump) {
				out("[table "+table+"]", fos, recordDelimiter);
			}
			if(doColumnNamesHeaderDump) {
				StringBuffer sb = new StringBuffer();
				for(int i=0;i<numCol;i++) {
					sb.append(md.getColumnName(i+1)+columnDelimiter);
				}
				out(sb.toString(), fos, recordDelimiter);
			}
			
			while(rs.next()) {
				out(SQLUtils.getRowFromRS(rs, numCol, table, columnDelimiter), fos, recordDelimiter);
			}
			rs.close();
			
			fos.close();
		}
		
	}
	
	void out(String s, PrintWriter pw, String recordDelimiter) throws IOException {
		pw.write(s+recordDelimiter);
	}
	
}
