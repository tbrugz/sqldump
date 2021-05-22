package tbrugz.sqldump.sqlrun.importers;

import java.util.Properties;

import tbrugz.sqldump.sqlrun.def.Constants;
import tbrugz.sqldump.sqlrun.def.Importer;

public class ImporterHelper {

	public static void setImporterPlainProperties(Importer importer, Properties prop) {
		String execId = "1";
		Properties p = new Properties();
		for(Object key: prop.keySet()) {
			p.setProperty(Constants.PREFIX_EXEC+execId+key, prop.getProperty(key.toString()));
		}
		importer.setExecId(execId);
		importer.setProperties(p);
	}
	
}
