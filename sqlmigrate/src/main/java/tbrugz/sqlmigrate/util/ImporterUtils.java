package tbrugz.sqlmigrate.util;

import java.io.File;
import java.io.IOException;

import tbrugz.sqldump.sqlrun.def.Importer;
import tbrugz.sqldump.sqlrun.importers.CSVImporter;
import tbrugz.sqldump.sqlrun.importers.ImporterHelper;
import tbrugz.sqldump.sqlrun.importers.XlsImporter;
import tbrugz.sqldump.util.ParametrizedProperties;

public class ImporterUtils {

	public static final String PROP_IMPORT = "import";

	static final String IMPORT_ID_CSV = "csv";
	static final String IMPORT_ID_XLS = "xls";

	public static Importer getImporter(File propertiesFile) throws IOException {
		ParametrizedProperties prop = new ParametrizedProperties();
		ParametrizedProperties.loadFile(prop, propertiesFile);

		String importerId = prop.getProperty(PROP_IMPORT);
		Importer importer = getImporterById(importerId);
		ImporterHelper.setImporterPlainProperties(importer, prop);
		
		return importer;
	}

	static Importer getImporterById(String id) {
		if(IMPORT_ID_CSV.equals(id)) {
			return new CSVImporter();
		}
		else if(IMPORT_ID_XLS.equals(id)) {
			return new XlsImporter();
		}
		//XXX: ffc? regex? 
		throw new IllegalArgumentException("Unknown importer [id: "+id+"]");
	}

}
