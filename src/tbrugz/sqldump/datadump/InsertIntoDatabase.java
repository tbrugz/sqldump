package tbrugz.sqldump.datadump;

import java.io.IOException;
import java.io.Writer;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import tbrugz.sqldump.def.ProcessingException;
import tbrugz.sqldump.util.ConnectionUtil;
import tbrugz.sqldump.util.MathUtil;
import tbrugz.sqldump.util.SQLUtils;
import tbrugz.sqldump.util.Utils;

public class InsertIntoDatabase extends InsertIntoDataDump implements DbUpdaterSyntax {

	static final Log log = LogFactory.getLog(InsertIntoDatabase.class);
	
	static final String IIDB_SYNTAX_ID = "iidb";
	static final String PREFIX_IIDB = "sqldump.datadump.iidb";
	
	static final String PROP_IIDB_CONN_PREFIX = PREFIX_IIDB+".connpropprefix";
	static final String PROP_IIDB_AUTOCOMMIT = PREFIX_IIDB+".autocommit";
	static final String PROP_IIDB_BATCH_MODE = PREFIX_IIDB+".batchmode";
	static final String PROP_IIDB_COMMIT_SIZE = PREFIX_IIDB+".commitsize";
	static final String PROP_IIDB_DROP_CREATE_TABLES = PREFIX_IIDB+".dropcreatetables";
	//was: writes insert to file if error when inserting into database (default is false)
	//static final String PROP_IIDB_FALLBACK_TO_FILE = PREFIX_IIDB+".fallbacktofile";
	
	//boolean fallbackToFile = false; //TODOne: remove the fallback...
	Connection conn = null;
	PreparedStatement stmt = null;
	
	boolean autoCommit = false;
	boolean batchMode = false;
	int commitSize = 100;
	
	long updated = 0;
	
	@Override
	public void initDump(String schemaName, String tableName, List<String> pkCols,
			ResultSetMetaData md) throws SQLException {
		super.initDump(schemaName, tableName, pkCols, md);

		//setup
		autoCommit = Utils.getPropBool(prop, PROP_IIDB_AUTOCOMMIT, autoCommit);
		batchMode = Utils.getPropBool(prop, PROP_IIDB_BATCH_MODE, batchMode);
		commitSize = Utils.getPropInt(prop, PROP_IIDB_COMMIT_SIZE, commitSize);
		//fallbackToFile = Utils.getPropBool(prop, PROP_IIDB_FALLBACK_TO_FILE, fallbackToFile);
		updated = 0;
		if(conn==null) {
			createConnection();
		}
		initSyntaxInternal();
	}
	
	void createConnection() {
		String connPropPrefix = prop.getProperty(PROP_IIDB_CONN_PREFIX);
		if(connPropPrefix==null) {
			throw new ProcessingException("connection prefix is null [prop '"+PROP_IIDB_CONN_PREFIX+"'], can't proceed");
		}
		//log.info("ac="+autoCommit+";batch="+batchMode+";commitsize="+commitSize+";fallback="+fallbackToFile);
		
		//create connection
		try {
			conn = ConnectionUtil.initDBConnection(connPropPrefix, prop, autoCommit);
		} catch (Exception e) {
			log.warn("error: "+e);
			throw new RuntimeException(e);
		}
		if(conn==null) {
			throw new RuntimeException("undefined connection properties");
		}
	}
	
	void initSyntaxInternal() throws SQLException {
		//drop & create table
		boolean dropCreateTables = Utils.getPropBool(prop, PROP_IIDB_DROP_CREATE_TABLES, false);
		if(dropCreateTables) {
			execSimpleSQL("drop table "+tableName, false);
			List<String> newcols = new ArrayList<String>();
			for(int i=0;i<lsColNames.size();i++) {
				newcols.add( lsColNames.get(i)+" "+getSQLType(lsColTypes.get(i)) );
			}
			String sqlcreate = "create table "+tableName+" ("
					+Utils.join(newcols, ", ")
					+")";
			execSimpleSQL(sqlcreate, true);
		}
		
		//create prepared statement
		List<String> vals = new ArrayList<String>();
		for(int i=0;i<lsColNames.size();i++) { vals.add("?"); }
		String sql = "insert into "+tableName+" "+colNames+" values ("+
				Utils.join(vals, ", ")+
				")";
		log.debug("stmt[autocommit="+autoCommit+";batch="+batchMode+"]: "+sql);
		stmt = conn.prepareStatement(sql);
	}
	
	@Override
	public void setUpdaterConnection(Connection conn) {
		this.conn = conn;
	}
	
	@Override
	public void dumpHeader() {
	}
	
	@Override
	public void dumpRow(ResultSet rs, long count)
			throws IOException, SQLException {
		List<Object> vals = SQLUtils.getRowObjectListFromRS(rs, lsColTypes, numCol, doDumpCursors);
		
		//try {

		//populate statement
		for(int i=0;i<lsColTypes.size();i++) {
			Class<?> type = lsColTypes.get(i);
			try {
				stmt.setObject(i+1, vals.get(i));
				//setParam(i, vals);
			}
			catch(RuntimeException e) {
				log.warn("error setting parameter [name="+lsColNames.get(i)+";type="+type.getName()+";value="+vals.get(i)+"]");
				throw e;
			}
		}
		
		//executeUpdate or addBatch
		if(batchMode) {
			stmt.addBatch();
		}
		else {
			updated += stmt.executeUpdate();
		}
		
		//commit - maybe
		if( (commitSize<=1) || (( (count+1) % commitSize)==0) ) {
			if(batchMode) {
				int[] updateCounts = stmt.executeBatch();
				updated += MathUtil.sumInts(updateCounts);
			}
			if(!autoCommit) {
				conn.commit();
			}
			log.debug("commit? "+ (count+1));
		}
		
		/*} catch (SQLException e) {
			if(fallbackToFile) {
				super.dumpRow(rs, count, fos);
			}
			else {
				throw e;
			}
		}*/
	}
	
	@Override
	public void dumpFooter(long count) throws IOException {
		try {
			//dumpFooterInternal(count);
			
			//executeUpdate - if batch_mode
			if(batchMode) {
				stmt.addBatch();
			}
			//commit
			if(!autoCommit) {
				conn.commit();
			}
			//close connection
			conn.close();
			
			log.debug("commit-last? count="+count+" updated="+updated);
			if(count!=updated) {
				log.warn("may not have imported all rows [count="+count+" updated="+updated+"]");
			}
		} catch (SQLException e) {
			//if(fallbackToFile) {
			//	super.dumpFooter(count, fos);
			//}
			//else {
				throw new ProcessingException(e);
			//}
		}
	}
	
	/*public void dumpFooterInternal(long count) throws SQLException {
		//executeUpdate - if batch_mode
		if(batchMode) {
			stmt.addBatch();
		}
		//commit
		if(!autoCommit) {
			conn.commit();
		}
		//close connection
		conn.close();
	}*/
	
	@Override
	public boolean isWriterIndependent() {
		return true; //!fallbackToFile;
	}
	
	@Override
	public String getSyntaxId() {
		return IIDB_SYNTAX_ID;
	}
	
	void execSimpleSQL(String sql, boolean warnOnException) {
		log.debug("sql: "+sql);
		try {
			conn.createStatement().execute(sql);
		}
		catch(SQLException e) {
			if(warnOnException) {
				log.warn("error: "+e);
			}
			else {
				log.debug("error: "+e);
			}
			if(!autoCommit) {
				try {
					conn.rollback();
				} catch (SQLException e1) {
					log.debug("error in rollback: "+e1);
				}
			}
		}
		
	}
	
	/*void setParam(int i, List<Object> vals) throws SQLException {
		Object o = vals.get(i);
		//stmt.setObject(i+1, o);
		if(o==null) {
			stmt.setNull(i+1, Types.NULL);
			return;
		}
		
		Class<?> oc = o.getClass();
		if(Integer.class.isAssignableFrom(oc)) {
			stmt.setInt(i+1, (Integer) vals.get(i));
		}
		else if(Long.class.isAssignableFrom(oc)) {
			stmt.setLong(i+1, (Long) vals.get(i));
		}
		else if(Double.class.isAssignableFrom(oc)) {
			stmt.setDouble(i+1, (Double) vals.get(i));
		}
		else if(Timestamp.class.isAssignableFrom(oc)) {
			stmt.setTimestamp(i+1, (Timestamp) vals.get(i));
		}
		else {
			stmt.setString(i+1, (String) vals.get(i));
		}
	}*/
	
	@Override
	public void dumpHeader(Writer fos) throws IOException {
		dumpHeader();
	}
	
	@Override
	public void dumpRow(ResultSet rs, long count, Writer fos) throws IOException, SQLException {
		dumpRow(rs, count);
	}
	
	@Override
	public void dumpFooter(long count, Writer fos) throws IOException {
		dumpFooter(count);
	}

	static String getSQLType(Class<?> type) {
		if(Integer.class.isAssignableFrom(type)) {
			return "INTEGER";
		}
		else if(Double.class.isAssignableFrom(type)) {
			return "DOUBLE";
		}
		else if(Date.class.isAssignableFrom(type)) {
			return "TIMESTAMP";
		}
		else {
			return "VARCHAR";
		}
	}

}
