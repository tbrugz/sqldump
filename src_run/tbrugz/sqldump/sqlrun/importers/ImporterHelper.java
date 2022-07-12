package tbrugz.sqldump.sqlrun.importers;

import java.util.Properties;

import tbrugz.sqldump.sqlrun.def.Constants;
import tbrugz.sqldump.sqlrun.def.Importer;
import tbrugz.sqldump.util.Utils;

public class ImporterHelper {

	static final String[] EXTENSIONS_XLS = {
		"xls", "xlsx"
	};

	static final String[] EXTENSIONS_DELIM = {
		"csv", "ssv", "scsv", "tsv", "psv"
	};
	
	//dot (.)
	static final String DOT = ".";

	public static void setImporterPlainProperties(Importer importer, Properties prop) {
		String execId = "1";
		Properties p = new Properties();
		for(Object key: prop.keySet()) {
			p.setProperty(Constants.PREFIX_EXEC+execId+DOT+key, prop.getProperty(key.toString()));
		}
		importer.setExecId(execId);
		importer.setProperties(p);
	}

	public static Importer getImporterByFileExt(final String ext) {
		return getImporterByFileExt(ext, null);
	}
	
	public static Importer getImporterByFileExt(final String ext, Properties prop) {
		if(ext==null) {
			throw new IllegalArgumentException("ext cannot be null");
		}

		Importer imp = null;
		if(prop==null) {
			prop = new Properties();
		}
		if(Utils.arrayContains(EXTENSIONS_XLS, ext)) {
			imp = new XlsImporter();
		}
		else if(Utils.arrayContains(EXTENSIONS_DELIM, ext)) {
			imp = new CSVImporter();
			if(ext.equals("csv")) {
				// do nothing
			}
			else if(ext.equals("ssv") || ext.equals("scsv")) {
				//setPlainProperty(imp, CSVImporter.SUFFIX_COLUMNDELIMITER, ";");
				prop.setProperty(CSVImporter.SUFFIX_COLUMNDELIMITER, ";");
			}
			else if(ext.equals("tsv")) {
				prop.setProperty(CSVImporter.SUFFIX_COLUMNDELIMITER, "\t");
			}
			else if(ext.equals("psv")) {
				prop.setProperty(CSVImporter.SUFFIX_COLUMNDELIMITER, "\\|");
			}
		}
		else {
			throw new IllegalArgumentException("unknown extension: "+ext);
		}
		setImporterPlainProperties(imp, prop);
		return imp;
	}
	
}
