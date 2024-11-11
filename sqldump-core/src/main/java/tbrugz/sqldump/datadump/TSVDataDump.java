package tbrugz.sqldump.datadump;

import java.util.Properties;

/**
 * See: 
 * https://en.wikipedia.org/wiki/Tab-separated_values
 * https://www.iana.org/assignments/media-types/text/tab-separated-values
 */
public class TSVDataDump extends CSVDataDump implements Cloneable {

	static final String TSV_SYNTAX_ID = "tsv";

	public static final String TSV_DELIM = "\t";

	@Override
	public void procProperties(Properties prop) {
		super.procProperties(prop);
		//procStandardProperties(prop);
		//recordDelimiter = prop.getProperty(fullPrefix() + SUFFIX_RECORDDELIMITER, DELIM_RECORD_DEFAULT);
		columnDelimiter = prop.getProperty(fullPrefix() + SUFFIX_COLUMNDELIMITER, TSV_DELIM);
		//postProcProperties();
	}

	@Override
	public String getSyntaxId() {
		return TSV_SYNTAX_ID;
	}

	@Override
	public String getDefaultFileExtension() {
		return "tsv"; // also '.tab'?
	}

	@Override
	public String getMimeType() {
		return "text/tab-separated-values";
	}

}
