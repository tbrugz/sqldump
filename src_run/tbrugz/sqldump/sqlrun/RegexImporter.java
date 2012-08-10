package tbrugz.sqldump.sqlrun;

import java.util.ArrayList;
import java.util.List;
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

		List<String> parts = new ArrayList<String>();
		if(matcher.find()) {
			for(int ii=1;ii<=matcher.groupCount();ii++) {
				parts.add(matcher.group(ii));
			}
		}
		log.debug("parts["+matcher.groupCount()+"/"+parts.size()+"]:: "+parts);
		if(processedLines==0) {
			StringBuffer sb = new StringBuffer();
			sb.append("insert into "+insertTable+ " values (");
			for(int i=0;i<parts.size();i++) {
				sb.append((i==0?"":", ")+"?");
			}
			sb.append(")");
			stmt = conn.prepareStatement(sb.toString());
		}
		return parts.toArray(NULL_STR_ARRAY);
	};
}
