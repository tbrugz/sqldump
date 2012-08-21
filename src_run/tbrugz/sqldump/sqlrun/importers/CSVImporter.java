package tbrugz.sqldump.sqlrun.importers;

import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class CSVImporter extends AbstractImporter {
	static final Log log = LogFactory.getLog(CSVImporter.class);

	static String SUFFIX_COLUMNDELIMITER = ".columndelimiter";

	static final String[] CSV_AUX_SUFFIXES = {
		SUFFIX_COLUMNDELIMITER
	};
	
	String columnDelimiter = ",";
	
	@Override
	void setImporterProperties(Properties prop, String importerPrefix) {
		super.setImporterProperties(prop, importerPrefix);
		columnDelimiter = prop.getProperty(importerPrefix+SUFFIX_COLUMNDELIMITER, columnDelimiter);
	}
	
	@Override
	public List<String> getAuxSuffixes() {
		List<String> ret = super.getAuxSuffixes();
		ret.addAll(Arrays.asList(CSV_AUX_SUFFIXES));
		return ret;
	}

	@Override
	String[] procLine(String line, long processedLines) throws SQLException {
		String[] parts = line.split(columnDelimiter);
		return parts;
	}
}
