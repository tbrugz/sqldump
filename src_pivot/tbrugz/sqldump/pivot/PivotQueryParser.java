package tbrugz.sqldump.pivot;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import tbrugz.sqldump.resultset.pivot.PivotResultSet;

/*
 * TODO: columns may have spaces if enclosed by '"'
 */
public class PivotQueryParser {

	static final String PATTERN_PIVOT_CLAUSE_STR = "/\\*\\s*pivot\\s+(.*)\\s+nonpivot\\s+(.*)\\s*\\*/";
	static final Pattern PATTERN_PIVOT_CLAUSE = Pattern.compile(PATTERN_PIVOT_CLAUSE_STR, Pattern.CASE_INSENSITIVE);
	
	static final String MEASURES_SHOW_LAST = "+measures-show-last";
	static final String MEASURES_SHOW_INROWS = "+measures-show-inrows";
	static final String MEASURES_SHOW_ALLWAYS = "+measures-show-allways";

	@SuppressWarnings("rawtypes")
	final Map<String, Comparable> colsToPivot;
	final List<String> colsNotToPivot;
	final int flags;
	
	@SuppressWarnings("rawtypes")
	public PivotQueryParser(String sql) {
		Matcher m = PATTERN_PIVOT_CLAUSE.matcher(sql);
		boolean found = m.find();
		if(!found) {
			colsToPivot = null;
			colsNotToPivot = null;
			flags = 0;
			return;
		}
		colsToPivot = new LinkedHashMap<String, Comparable>();
		colsNotToPivot = new ArrayList<String>();
		
		String pivotColsStr = m.group(1);
		String[] pivotCols = pivotColsStr.split(",");
		for(String s: pivotCols) {
			colsToPivot.put(s.trim(), null); //XXX: add comparator? asc, desc?
		}
		
		String nonPivotColsStr = m.group(2); 
		String[] nonPivotCols = nonPivotColsStr.split("[, ]");
		int flags = 0;
		for(String s: nonPivotCols) {
			s = s.trim();
			if("".equals(s)) { continue; }
			if(s.startsWith("+")) {
				if(MEASURES_SHOW_LAST.equals(s)) {
					flags |= PivotResultSet.SHOW_MEASURES_LAST;
				}
				else if(MEASURES_SHOW_INROWS.equals(s)) {
					flags |= PivotResultSet.SHOW_MEASURES_IN_ROWS;
				}
				else if(MEASURES_SHOW_ALLWAYS.equals(s)) {
					flags |= PivotResultSet.SHOW_MEASURES_ALLWAYS;
				}
				else {
					throw new IllegalArgumentException("unknown control argument: "+s);
				}
			}
			else {
				colsNotToPivot.add(s);
			}
		}
		this.flags = flags;
		//System.out.println("flags: "+flags);
	}
	
	@Override
	public String toString() {
		return "PivotQueryParser[colsToPivot="+colsToPivot+";colsNotToPivot="+colsNotToPivot+";flags="+flags+"]";
	}
	
}
