package tbrugz.sqldump.sqlrun.importers;

import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import tbrugz.sqldump.util.Utils;

public class CSVImporter extends AbstractImporter {
	static final Log log = LogFactory.getLog(CSVImporter.class);

	static String SUFFIX_COLUMNDELIMITER = ".columndelimiter";
	static String SUFFIX_EMPTY_STRING_AS_NULL = ".emptystringasnull";

	static final String[] CSV_AUX_SUFFIXES = {
		SUFFIX_COLUMNDELIMITER
	};
	
	String columnDelimiter = ",";
	boolean setNullWhenEmptyString = false;
	
	@Override
	void setImporterProperties(Properties prop, String importerPrefix) {
		super.setImporterProperties(prop, importerPrefix);
		columnDelimiter = prop.getProperty(importerPrefix+SUFFIX_COLUMNDELIMITER, columnDelimiter);
		setNullWhenEmptyString = Utils.getPropBool(prop, importerPrefix+SUFFIX_EMPTY_STRING_AS_NULL, setNullWhenEmptyString);
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
		if(setNullWhenEmptyString && parts!=null) {
			for(int i=0;i<parts.length;i++) {
				if("".equals(parts[i])) {
					parts[i] = null;
				}
			}
		}
		return parts;
	}
}
