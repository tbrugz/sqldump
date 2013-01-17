package tbrugz.sqldump.sqlrun.importers;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import tbrugz.sqldump.util.Utils;

public class RegexImporter extends AbstractImporter {
	static final Log log = LogFactory.getLog(RegexImporter.class);
	
	static final String SUFFIX_PATTERN = ".pattern";
	static final String SUFFIX_PATTERNFLAGS = ".patternflags";
	
	static final String[] NULL_STR_ARRAY = {};
	
	static final String[] REGEX_AUX_SUFFIXES = {
		SUFFIX_PATTERN,
		SUFFIX_PATTERNFLAGS
	};

	String patternStr = null;
	Pattern pattern = null;
	int patternFlags = 0;
	
	List<Integer> loggedPatternFailoverIds = new ArrayList<Integer>();

	@Override
	public void setImporterProperties(Properties prop, String importerPrefix) {
		super.setImporterProperties(prop, importerPrefix);
		patternStr = prop.getProperty(importerPrefix+SUFFIX_PATTERN, patternStr);
		patternFlags = Utils.getPropInt(prop, importerPrefix+SUFFIX_PATTERNFLAGS, patternFlags);
		pattern = Pattern.compile(patternStr, patternFlags);
		
		if(!loggedPatternFailoverIds.contains(failoverId)) {
			log.info("pattern"+(failoverId>0?"[failover="+failoverId+"]":"")+": "+patternStr);
			loggedPatternFailoverIds.add(failoverId);
		}
	}

	@Override
	public List<String> getAuxSuffixes() {
		List<String> ret = super.getAuxSuffixes();
		ret.addAll(Arrays.asList(REGEX_AUX_SUFFIXES));
		return ret;
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
