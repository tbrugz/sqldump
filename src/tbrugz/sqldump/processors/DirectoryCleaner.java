package tbrugz.sqldump.processors;

import java.io.File;
import java.util.Properties;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import tbrugz.sqldump.SQLDump;
import tbrugz.sqldump.def.AbstractProcessor;
import tbrugz.sqldump.util.Utils;

//XXX: dir prop/dirToDeleteFiles must be absolute ?
public class DirectoryCleaner extends AbstractProcessor {

	static final Log log = LogFactory.getLog(DirectoryCleaner.class);
	
	File dirToDeleteFiles = null;
	
	@Override
	public void setProperties(Properties prop) {
		String dir = prop.getProperty(SQLDump.PROP_DO_DELETEREGULARFILESDIR);
		if( dir!=null && !"".equals(dir) ) {
			dirToDeleteFiles = new File(dir);
		}
	}
	
	@Override
	public boolean needsConnection() {
		return false;
	}

	@Override
	public void process() {
		if(dirToDeleteFiles==null) {
			log.warn("no directory defined for cleaning");
			return;
		}
		
		log.info("deleting regular files from dir: "+dirToDeleteFiles.getAbsolutePath());
		int delCount = Utils.deleteDirRegularContents(dirToDeleteFiles);
		if(delCount>0) {
			log.info(delCount+" files deleted");
		}
		else {
			log.info("no files deleted");
		}
	}

	public File getDirToDeleteFiles() {
		return dirToDeleteFiles;
	}

	public void setDirToDeleteFiles(File dirToDeleteFiles) {
		this.dirToDeleteFiles = dirToDeleteFiles;
	}
	
}
