package tbrugz.sqldump.sqlrun.def;

import java.io.File;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import tbrugz.sqldump.util.FileUtils;

public class Util {

	static final Log log = LogFactory.getLog(Util.class);
	
	static final Log logCommit = LogFactory.getLog("tbrugz.sqldump.sqlrun.commit");
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
	
	@Deprecated
	public static List<String> getFiles(String dir, String fileRegex) {
		return FileUtils.getFilesRegex(new File(dir), fileRegex);
	}

}
