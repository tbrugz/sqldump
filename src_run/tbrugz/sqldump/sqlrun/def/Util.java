package tbrugz.sqldump.sqlrun.def;

import java.io.File;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

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

	public static List<String> getFiles(String dir, String fileRegex) {
		List<String> ret = new ArrayList<String>();
		if(dir==null) {
			log.warn("dir '"+dir+"' not found...");
			return null;
		}
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
