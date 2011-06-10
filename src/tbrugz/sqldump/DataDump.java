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

	static Logger log = Logger.getLogger(SQLDump.class);
	
	void dumpData(Connection conn, List<String> tableNamesForDataDump, Properties prop) throws Exception {
		FileWriter fos = new FileWriter(prop.getProperty(PROP_DATADUMP_FILEPATTERN));
		log.info("data dumping...");
		
		for(String table: tableNamesForDataDump) {
			Statement st = conn.createStatement();
			log.info("dumping data from table: "+table);
			ResultSet rs = st.executeQuery("select * from \""+table+"\"");
			out("\n[table "+table+"]\n", fos);
			ResultSetMetaData md = rs.getMetaData();
			int numCol = md.getColumnCount();
			while(rs.next()) {
				out(SQLUtils.getRowFromRS(rs, numCol, table), fos);
			}
			rs.close();
		}
		
		fos.close();
	}
	
	void out(String s, FileWriter fos) throws IOException {
		fos.write(s+"\n");
	}
	
}
