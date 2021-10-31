package tbrugz.sqldump.sqlrun.importers;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

import javax.naming.NamingException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import tbrugz.sqldump.datadump.DataDump;
import tbrugz.sqldump.datadump.DumpSyntax;
import tbrugz.sqldump.datadump.InsertIntoDatabase;
import tbrugz.sqldump.sqlrun.def.Constants;
import tbrugz.sqldump.util.ConnectionUtil;
import tbrugz.sqldump.util.IOUtil;

public class SqlImporter extends BaseImporter {

	static final Log log = LogFactory.getLog(SqlImporter.class);

	static final String SUFFIX_READ_CONN_PREFIX = ".read-connection-prefix";
	static final String SUFFIX_SQL = ".sql";
	static final String SUFFIX_SQLFILE = ".sqlfile";

	static final String[] SQLI_AUX_SUFFIXES = {
		SUFFIX_READ_CONN_PREFIX, SUFFIX_SQL, SUFFIX_SQLFILE
	};
	
	String sql = null;
	String readConnPropPrefix = null;
	
	@Override
	public List<String> getAuxSuffixes() {
		return Arrays.asList(SQLI_AUX_SUFFIXES);
	}
	
	@Override
	public void setProperties(Properties prop) {
		super.setProperties(prop);
		
		// limit/offset: .limit/.skipnlines - may be better to define in query
		
		// .sql
		sql = prop.getProperty(Constants.PREFIX_EXEC+execId+SUFFIX_SQL);
		if(sql==null) {
			// .sqlfile
			String sqlFile = prop.getProperty(Constants.PREFIX_EXEC+execId+SUFFIX_SQLFILE);
			if(sqlFile!=null) {
				try {
					sql = IOUtil.readFromReader(new FileReader(sqlFile));
				} catch (IOException e) {
					log.warn("Exception: "+e);
				}
			}
		}
		
		readConnPropPrefix = prop.getProperty(Constants.PREFIX_EXEC+execId+SUFFIX_READ_CONN_PREFIX);
	}
	
	@Override
	public long importData() throws SQLException, InterruptedException, IOException {
		List<DumpSyntax> syntaxList = new ArrayList<DumpSyntax>();
		syntaxList.add(getInsertIntoSyntax());
		Connection readConn = getReadConnection();
		List<Object> params = null;
		long rowlimit = DataDump.getTableRowLimit(prop, insertTable);
		String schemaName = null, tableOrQueryId = insertTable, tableOrQueryName = insertTable, charset = null;
		
		DataDump dd = new DataDump();
		
		try {
			long count = dd.runQuery(readConn, sql, 
				params,
				prop,
				schemaName, tableOrQueryId, tableOrQueryName, charset,
				rowlimit, syntaxList
				);
			return count;
		}
		finally {
			ConnectionUtil.closeConnection(readConn);
		}
	}
	
	@Override
	public long importStream(InputStream is) throws SQLException, InterruptedException, IOException {
		throw new UnsupportedOperationException("importStream()");
	}
	
	DumpSyntax getInsertIntoSyntax() {
		InsertIntoDatabase iidb = new InsertIntoDatabase();
		iidb.setUpdaterConnection(conn);
		iidb.procProperties(prop);
		//iidb.setFeatures(...);
		if(insertSQL != null) {
			log.info("setting insert/update sql: "+insertSQL);
			iidb.setUpdateSql(insertSQL);
		}
		return iidb;
	}
	
	Connection getReadConnection() throws SQLException {
		try {
			Connection readConn = ConnectionUtil.initDBConnection(readConnPropPrefix, prop);
			return readConn;
		} catch (ClassNotFoundException e) {
			log.warn("Exception: "+e);
			throw new RuntimeException(e);
		} catch (NamingException e) {
			log.warn("Exception: "+e);
			throw new RuntimeException(e);
		}
	}
	
	@Override
	public long importFilesGlob(String filesGlobPattern, File importDir)
			throws SQLException, InterruptedException, IOException {
		throw new UnsupportedOperationException("importFilesGlob()");
	}

}
