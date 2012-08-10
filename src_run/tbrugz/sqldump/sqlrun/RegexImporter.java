package tbrugz.sqldump.sqlrun;

import java.util.Properties;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class RegexImporter extends CSVImporter {
	static final Log log = LogFactory.getLog(RegexImporter.class);
	
	static final String SUFFIX_PATTERN = ".pattern";
	static final String[] NULL_STR_ARRAY = {};
	
	static final String[] REGEX_AUX_SUFFIXES = {
		SUFFIX_PATTERN
	};

	String patternStr = null;
	Pattern pattern = null;
	
	@Override
	public void setProperties(Properties prop) {
		super.setProperties(prop);
		patternStr = prop.getProperty(SQLRun.PREFIX_EXEC+execId+SUFFIX_PATTERN);
		pattern = Pattern.compile(patternStr);
	}
	
	@Override
	public String[] getAuxSuffixes() {
		return REGEX_AUX_SUFFIXES;
	}
	
	@Override
	String[] procLine(String line, long processedLines) throws java.sql.SQLException {
		Matcher matcher = pattern.matcher(line);

		String[] parts = null;
		if(matcher.find()) {
			parts = new String[matcher.groupCount()];
			for(int ii=1;ii<=matcher.groupCount();ii++) {
				parts[ii-1] = matcher.group(ii);
			}
		}
		return parts;
	}
}
