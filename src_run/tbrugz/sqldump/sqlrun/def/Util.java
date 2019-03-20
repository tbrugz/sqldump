package tbrugz.sqldump.sqlrun.def;

import java.io.File;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.Savepoint;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class Util {

	static final Log log = LogFactory.getLog(Util.class);
	
	static final Log logCommit = LogFactory.getLog("tbrugz.sqldump.sqlrun.commit");
	static final Log logRollback = LogFactory.getLog("tbrugz.sqldump.sqlrun.rollback");
	public static final Log logBatch = LogFactory.getLog("tbrugz.sqldump.sqlrun.batch");
	
	public static void doCommit(Connection conn) {
		try {
			//log.debug("committing...");
			conn.commit();
			logCommit.debug("committed!");
		} catch (SQLException e) {
			logCommit.warn("error commiting: "+e);
		}
	}
	
	public static void doRollback(Connection conn) {
		try {
			//log.debug("rolling back...");
			conn.rollback();
			logRollback.debug("rolled back!");
		} catch (SQLException e) {
			logRollback.warn("error rollbacking: "+e);
		}
	}

	public static void doRollback(Connection conn, Savepoint savepoint) {
		try {
			//log.debug("rolling back...");
			conn.rollback(savepoint);
			logRollback.debug("rolled back with savepoint! [id="+savepoint.getSavepointId()+"]");
		} catch (SQLException e) {
			logRollback.warn("error rollbacking with savepoint: "+e);
		}
	}
	
	public static boolean releaseSavepoint(Connection conn, Savepoint sp) {
		try {
			if(sp!=null) {
				conn.releaseSavepoint(sp);
			}
		} catch (SQLFeatureNotSupportedException e) {
			log.debug("Error releasing savepoint: "+e);
		} catch (SQLException e) {
			log.warn("Error releasing savepoint: "+e);
			log.debug("Error releasing savepoint: "+e.getMessage(), e);
			return false;
		}
		return true;
	}
	
	public static List<String> getFiles(String dir, String fileRegex) {
		if(dir==null) {
			log.warn("dir cannot be null");
			return null;
		}
		List<String> ret = new ArrayList<String>();
		File fdir = new File(dir);
		String[] files = fdir.list();
		if(files==null) {
			return null;
		}
		for(String file: files) {
			if(file.matches(fileRegex)) {
				ret.add(fdir.getAbsolutePath()+File.separator+file);
			}
		}
		return ret;
	}

}
