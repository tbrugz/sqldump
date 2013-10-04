package tbrugz.sqldump.pivot;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class PivotQueryParser {

	/*static PivotQueryParser parser = new PivotQueryParser();
	
	public static PivotQueryParser instance() {
		return parser;
	}
	
	public ResultSet parse(String sql) {
		return null;
	}*/
	
	static final String PATTERN_PIVOT_CLAUSE_STR = "/\\*\\s*pivot\\s+(.*)\\s+nonpivot\\s+(.*)\\s*\\*/";
	static final Pattern PATTERN_PIVOT_CLAUSE = Pattern.compile(PATTERN_PIVOT_CLAUSE_STR, Pattern.CASE_INSENSITIVE);

	final Map<String, Comparable> colsToPivot;
	final List<String> colsNotToPivot;
	
	public PivotQueryParser(String sql) {
		Matcher m = PATTERN_PIVOT_CLAUSE.matcher(sql);
		boolean found = m.find();
		if(!found) {
			colsToPivot = null;
			colsNotToPivot = null;
			return;
		}
		colsToPivot = new LinkedHashMap<String, Comparable>();
		colsNotToPivot = new ArrayList<String>();
		
		String pivotColsStr = m.group(1);
		String[] pivotCols = pivotColsStr.split(",");
		for(String s: pivotCols) {
			colsToPivot.put(s.trim(), null);
		}
		
		String nonPivotColsStr = m.group(2); 
		String[] nonPivotCols = nonPivotColsStr.split(",");
		for(String s: nonPivotCols) {
			colsNotToPivot.add(s.trim());
		}
	}
	
}
