package tbrugz.sqldump.sqlrun.importers;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.sql.SQLException;
import java.util.List;
import java.util.Properties;

import tbrugz.sqldump.sqlrun.def.Constants;
import tbrugz.sqldump.util.FileUtils;

public abstract class BaseFileImporter extends BaseImporter {

	String importFile = null;
	File importDir = null;
	String importFilesGlob = null;

	@Override
	public void setProperties(Properties prop) {
		super.setProperties(prop);
		
		importFile = prop.getProperty(Constants.PREFIX_EXEC+execId+Constants.SUFFIX_IMPORTFILE);
		String importDirStr = prop.getProperty(Constants.PREFIX_EXEC+execId+Constants.SUFFIX_IMPORTDIR);
		if(importDirStr!=null) {
			importDir = new File(importDirStr);
		}
		String importFiles = prop.getProperty(Constants.PREFIX_EXEC+execId+Constants.SUFFIX_IMPORTFILES);
		importFilesGlob = prop.getProperty(Constants.PREFIX_EXEC+execId+Constants.SUFFIX_IMPORTFILES_GLOB);
		if(importFiles!=null && importFilesGlob==null) {
			// importFilesGlob is the default
			importFilesGlob = importFiles;
		}
	}
	
	public long importFilesGlob(String filesGlobPattern, File importDir) throws IOException, SQLException, InterruptedException {
		long ret = 0;
		if(importDir==null) {
			importDir = FileUtils.getInitDirForPath(filesGlobPattern);
		}
		log.info("importing files with glob pattern '"+filesGlobPattern+"' from dir '"+importDir+"'");
		List<String> files = FileUtils.getFilesGlobAsString(importDir, filesGlobPattern);
		if(files==null || files.size()==0) {
			log.warn("no files in dir '"+importDir+"'...");
		}
		else {
			for(String file: files) {
				log.info("importing file: "+file);
				ret += importStream(new FileInputStream(file));
				//filesImported++;
			}
		}
		return ret;
	}
	
}
