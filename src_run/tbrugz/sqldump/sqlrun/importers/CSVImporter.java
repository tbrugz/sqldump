package tbrugz.sqldump.sqlrun.importers;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import tbrugz.sqldump.util.Utils;

public class CSVImporter extends AbstractImporter {
	
	static final Log log = LogFactory.getLog(CSVImporter.class);

	static final String SUFFIX_COLUMNDELIMITER = ".columndelimiter";
	static final String SUFFIX_EMPTY_STRING_AS_NULL = ".emptystringasnull";
	static final String SUFFIX_NULL_CONSTANT = ".nullconstant";
	static final String SUFFIX_NULL_CONSTANTS = ".nullconstants";
	
	static final String DOUBLEQUOTE = "\"";

	static final String[] EMPTY_STRING_ARRAY = {};

	static final String[] CSV_AUX_SUFFIXES = {
		SUFFIX_COLUMNDELIMITER, SUFFIX_NULL_CONSTANT
	};
	
	String columnDelimiter = ",";
	@Deprecated boolean setNullWhenEmptyString = false;
	String nullConstant = null;
	List<String> nullConstants = null;
	
	String lastLine = null;
	Boolean lastLineComplete = null;
	
	@Override
	void setImporterProperties(Properties prop, String importerPrefix) {
		super.setImporterProperties(prop, importerPrefix);
		columnDelimiter = prop.getProperty(importerPrefix+SUFFIX_COLUMNDELIMITER, columnDelimiter);
		setNullWhenEmptyString = Utils.getPropBool(prop, importerPrefix+SUFFIX_EMPTY_STRING_AS_NULL, setNullWhenEmptyString);
		nullConstant = prop.getProperty(importerPrefix+SUFFIX_NULL_CONSTANT);
		nullConstants = Utils.getStringListFromProp(prop, importerPrefix+SUFFIX_NULL_CONSTANTS, ",");
	}
	
	@Override
	public List<String> getAuxSuffixes() {
		List<String> ret = super.getAuxSuffixes();
		ret.addAll(Arrays.asList(CSV_AUX_SUFFIXES));
		return ret;
	}

	
	// TODOne: parse strings inside quotes '"'
	@Override
	String[] procLine(String line, long processedLines) throws SQLException {
		lastLine = line;
		lastLineComplete = null;
		if(!isLastLineComplete()) { return null; }
		
		String[] parts = line.split(columnDelimiter, -1);
		List<String> pl = new ArrayList<String>();
		StringBuilder sb = new StringBuilder();
		for(String s: parts) {
			sb.append(s);
			if( countCharacters(sb.toString(), '"')%2==0 ) {
				pl.add(stringValue(sb.toString()));
				sb = new StringBuilder();
			}
			else { 
				sb.append(columnDelimiter);
			}
		}
		parts = pl.toArray(EMPTY_STRING_ARRAY);
		
		if(setNullWhenEmptyString && parts!=null) {
			for(int i=0;i<parts.length;i++) {
				if("".equals(parts[i])) {
					parts[i] = null;
				}
			}
		}
		if(nullConstant!=null && parts!=null) {
			for(int i=0;i<parts.length;i++) {
				if(nullConstant.equals(parts[i])) {
					parts[i] = null;
				}
			}
		}
		if(nullConstants!=null && parts!=null) {
			for(int i=0;i<parts.length;i++) {
				if(nullConstants.contains(parts[i])) {
					parts[i] = null;
				}
			}
		}
		return parts;
	}
	
	public static String stringValue(String value) {
		if(value.startsWith(DOUBLEQUOTE) && value.endsWith(DOUBLEQUOTE)) {
			value = value.substring(1, value.length()-1).replaceAll(DOUBLEQUOTE+DOUBLEQUOTE, DOUBLEQUOTE);
		}
		return value;
	}
	
	@Override
	boolean isLastLineComplete() {
		if(lastLineComplete!=null) { return lastLineComplete; }
		int count = countCharacters(lastLine, '"');
		lastLineComplete = count%2==0;
		//log.info("line[#quote="+count+";complete="+lastLineComplete+"]: "+lastLine);
		return lastLineComplete;
	}

	static int countCharacters(String s, char c) {
		int count = 0;
		for(int i=0;i<s.length();i++) {
			if(s.charAt(i)==c) { count++; }
		}
		return count;
	}
	
	@Override
	String recordDelimiterReplacer() {
		return "\n";
	}
	
}
