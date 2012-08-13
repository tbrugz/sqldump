package tbrugz.sqldump.sqlrun.importers;

import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import tbrugz.sqldump.sqlrun.SQLRun;

public class CSVImporter extends AbstractImporter {
	static final Log log = LogFactory.getLog(CSVImporter.class);

	static String SUFFIX_COLUMNDELIMITER = ".columndelimiter";

	static final String[] CSV_AUX_SUFFIXES = {
		SUFFIX_COLUMNDELIMITER
	};
	
	String columnDelimiter = ",";
	String recordDelimiter = "\n";
	
	@Override
	public void setProperties(Properties prop) {
		super.setProperties(prop);
		recordDelimiter = prop.getProperty(SQLRun.PREFIX_EXEC+execId+SUFFIX_RECORDDELIMITER, recordDelimiter);
		columnDelimiter = prop.getProperty(SQLRun.PREFIX_EXEC+execId+SUFFIX_COLUMNDELIMITER, columnDelimiter);
	}

	@Override
	public List<String> getAuxSuffixes() {
		List<String> ret = super.getAuxSuffixes();
		ret.addAll(Arrays.asList(CSV_AUX_SUFFIXES));
		return ret;
	}

	String[] procLine(String line, long processedLines) throws SQLException {
		String[] parts = line.split(columnDelimiter);
		return parts;
	}
}
