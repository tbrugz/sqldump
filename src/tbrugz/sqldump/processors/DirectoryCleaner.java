package tbrugz.sqldump.processors;

import java.io.File;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import tbrugz.sqldump.def.AbstractSQLProc;
import tbrugz.sqldump.util.Utils;

public class DirectoryCleaner extends AbstractSQLProc {

	static final Log log = LogFactory.getLog(DirectoryCleaner.class);
	
	File dirToDeleteFiles = null;
	
	@Override
	public boolean needsConnection() {
		return false;
	}

	@Override
	public void process() {
		log.info("deleting regular files from dir: "+dirToDeleteFiles.getAbsolutePath());
		int delCount = Utils.deleteDirRegularContents(dirToDeleteFiles);
		if(delCount>0) {
			log.info(delCount+" files deteted");
		}
		else {
			log.info("no files deteted");
		}
	}

	public File getDirToDeleteFiles() {
		return dirToDeleteFiles;
	}

	public void setDirToDeleteFiles(File dirToDeleteFiles) {
		this.dirToDeleteFiles = dirToDeleteFiles;
	}
	
}
