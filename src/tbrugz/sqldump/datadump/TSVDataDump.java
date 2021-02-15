package tbrugz.sqldump.datadump;

import java.util.Properties;

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
		return "tsv";
	}

}
