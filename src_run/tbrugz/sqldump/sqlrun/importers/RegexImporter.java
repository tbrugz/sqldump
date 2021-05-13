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
	static final String SUFFIX_SUBPATTERNS2IGNORE = ".subpatterns2ignore";
	
	static final String[] NULL_STR_ARRAY = {};
	
	static final String[] REGEX_AUX_SUFFIXES = {
		SUFFIX_PATTERN,
		SUFFIX_PATTERNFLAGS,
		SUFFIX_SUBPATTERNS2IGNORE
	};

	String patternStr = null;
	Pattern pattern = null;
	int patternFlags = 0;
	List<Pattern> patterns2ignore = new ArrayList<Pattern>();
	
	List<Integer> loggedPatternFailoverIds = new ArrayList<Integer>();

	@Override
	public void setImporterProperties(Properties prop, String importerPrefix) {
		super.setImporterProperties(prop, importerPrefix);
		patternStr = prop.getProperty(importerPrefix+SUFFIX_PATTERN, patternStr);
		patternFlags = Utils.getPropInt(prop, importerPrefix+SUFFIX_PATTERNFLAGS, patternFlags);
		pattern = Pattern.compile(patternStr, patternFlags);
		List<String> patterns2ignoreStr = Utils.getStringListFromProp(prop, importerPrefix+SUFFIX_SUBPATTERNS2IGNORE, "\\|");
		if(patterns2ignoreStr!=null) {
			//log.info("pats2ign: "+patterns2ignoreStr);
			for(String s: patterns2ignoreStr) {
				patterns2ignore.add(Pattern.compile(s));
			}
		}
		
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
		for(Pattern ignp: patterns2ignore) {
			line = ignp.matcher(line).replaceAll("");
		}
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
	
	@Override
	boolean isLastLineComplete() {
		return true;
	}

	@Override
	String recordDelimiterReplacer() {
		return null;
	}
	
}
