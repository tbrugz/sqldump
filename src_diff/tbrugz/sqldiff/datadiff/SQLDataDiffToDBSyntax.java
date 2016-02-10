package tbrugz.sqldiff.datadiff;

import java.io.IOException;
import java.io.Writer;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.List;
import java.util.Properties;

import javax.naming.NamingException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import tbrugz.sqldump.def.ProcessingException;
import tbrugz.sqldump.util.ConnectionUtil;

/*
 * TODO: option to use PreparedStatements (one for each datadiff type - insert, update, delete)
 */
public class SQLDataDiffToDBSyntax extends SQLDataDiffSyntax {

	static final Log log = LogFactory.getLog(SQLDataDiffToDBSyntax.class);
	
	static final String PREFIX_SDD2DB = "sqldiff.datadiff.sdd2db";
	static final String PROP_SDD2DB_CONN_PREFIX = PREFIX_SDD2DB+".connpropprefix";

	String connPropPrefix;
	Properties prop;
	
	boolean autoCommit = false; //XXX: add prop for autoCommit
	int commitSize = 100; //XXX: add prop for commit size
	
	Connection conn;
	int updateCount = 0;
	
	@Override
	public void procProperties(Properties prop) {
		super.procProperties(prop);
		this.prop = prop;
		connPropPrefix = prop.getProperty(PROP_SDD2DB_CONN_PREFIX);
		//XXX: option to use props 'sqldiff.applydifftosource' & 'sqldiff.applydiffto' ?
		if(connPropPrefix==null) {
			throw new ProcessingException("connection prefix is null [prop '"+PROP_SDD2DB_CONN_PREFIX+"'], can't proceed");
		}
	}
	
	
	@Override
	public void initDump(String schemaName, String tableName, List<String> pkCols,
			ResultSetMetaData md) throws SQLException {
		super.initDump(schemaName, tableName, pkCols, md);
		try {
			conn = ConnectionUtil.initDBConnection(connPropPrefix, prop, autoCommit);
		} catch (ClassNotFoundException e) {
			log.warn("Error: "+e);
		} catch (NamingException e) {
			log.warn("Error: "+e);
		}
	}
	
	@Override
	public void dumpHeader() throws IOException {
		super.dumpHeader((Writer) null);
	}
	
	@Override
	public void dumpRow(ResultSet rs, long count) throws IOException,
			SQLException {
		super.dumpRow(rs, count, (Writer) null);
	}
	
	@Override
	public void dumpFooter(long count) throws IOException {
		super.dumpFooter(count, (Writer) null);
		try {
			conn.commit();
		} catch (SQLException e) {
			log.warn("Error: "+e);
		}
	}
	
	@Override
	protected void out(String s, Writer pw) throws IOException {
		try {
			conn.prepareStatement(s).executeUpdate();
			updateCount++;
			if( (updateCount % commitSize) == 0) {
				conn.commit();
			}
		} catch (SQLException e) {
			log.warn("Error: "+e);
		}
	}
	
	@Override
	public boolean isWriterIndependent() {
		return true;
	}
	
}
