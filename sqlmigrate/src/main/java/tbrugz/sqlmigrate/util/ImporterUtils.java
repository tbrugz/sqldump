package tbrugz.sqlmigrate.util;

import java.io.File;
import java.io.IOException;

import tbrugz.sqldump.sqlrun.def.Importer;
import tbrugz.sqldump.sqlrun.importers.ImporterHelper;
import tbrugz.sqldump.util.ParametrizedProperties;

public class ImporterUtils {

	public static final String PROP_IMPORT = "import";

	static final String IMPORT_ID_CSV = "csv";
	static final String IMPORT_ID_XLS = "xls";

	public static Importer getImporter(File propertiesFile) throws IOException {
		return getImporter(propertiesFile, null);
	}

	public static Importer getImporter(File propertiesFile, Boolean failonerror) throws IOException {
		ParametrizedProperties prop = new ParametrizedProperties();
		ParametrizedProperties.loadFile(prop, propertiesFile);

		String importerId = prop.getProperty(PROP_IMPORT);
		Importer importer = getImporterById(importerId);
		if(failonerror!=null) {
			importer.setFailOnError(failonerror);
		}
		ImporterHelper.setImporterPlainProperties(importer, prop);
		
		return importer;
	}

	static Importer getImporterById(String id) {
		if(IMPORT_ID_CSV.equals(id)) {
			//return new CSVImporter();
			return ImporterHelper.getImporterInstance(id);
		}
		else if(IMPORT_ID_XLS.equals(id)) {
			//return new XlsImporter();
			return ImporterHelper.getImporterInstance(id);
		}
		//XXX: ffc? regex?
		throw new IllegalArgumentException("Unknown importer [id: "+id+"]");
	}

}
