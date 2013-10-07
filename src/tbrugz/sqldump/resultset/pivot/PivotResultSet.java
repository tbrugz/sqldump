package tbrugz.sqldump.resultset.pivot;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.regex.Pattern;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import tbrugz.sqldump.resultset.AbstractResultSet;
import tbrugz.sqldump.resultset.RSMetaDataAdapter;

/*
 * TODO: after PivotRS is built, add possibility for change key-col <-> key-row (pivotting)
 * TODO: multiple column type (beside String): Integer, Double, Date
 * TODO: allow null key (pivotted) values (add StringComparatorNullFirst/Last ?)
 * XXX: option to remove cols/rows/both where all measures are null
 * XXX: aggregate if duplicated key found? first(), last()?
 */
@SuppressWarnings("rawtypes")
public class PivotResultSet extends AbstractResultSet {
	
	final static Log log = LogFactory.getLog(PivotResultSet.class);

	final static String COLS_SEP = "|";
	final static String COLS_SEP_PATTERN = Pattern.quote(COLS_SEP);
	final static String COLVAL_SEP = ":";
	final static String COLVAL_SEP_PATTERN = Pattern.quote(COLVAL_SEP);

	final static int logEachXRows = 1000;
	
	final ResultSet rs;
	final int rsColsCount;
	
	final List<String> colsNotToPivot;
	final Map<String, Comparable> colsToPivot;
	final List<String> measureCols;

	final List<String> colsToPivotNames;
	
	final Map<String, Set<String>> keyColValues = new HashMap<String, Set<String>>();
	final List<Map<String, String>> valuesForEachMeasure = new ArrayList<Map<String,String>>(); //new HashMap<String, String>();
	final List<String> newColNames = new ArrayList<String>();
	final ResultSetMetaData metadata;
	
	boolean processed = false;
	int position = -1;
	int originalRSRowCount = 0;
	int rowCount = 0;
	final List<String> nonPivotKeyValues = new ArrayList<String>();
	//final List<List<String>> nonPivotValues = new ArrayList<List<String>>();
	String currentNonPivotKey = null;
	public boolean showMeasuresInColumns = true;
	public boolean showMeasuresFirst = true;
	public boolean alwaysShowMeasures = false;

	//colsNotToPivot - key cols
	public PivotResultSet(ResultSet rs, List<String> colsNotToPivot, List<String> colsToPivot, boolean doProcess) throws SQLException {
		this(rs, colsNotToPivot, list2Map(colsToPivot), doProcess);
	}
	
	public PivotResultSet(ResultSet rs, List<String> colsNotToPivot, Map<String, Comparable> colsToPivot,
			boolean doProcess) throws SQLException {
		this.rs = rs;
		this.colsNotToPivot = colsNotToPivot;
		this.colsToPivot = colsToPivot;
		
		List<String> colsTP = new ArrayList<String>();
		for(String key: colsToPivot.keySet()) {
			colsTP.add(key);
		}
		colsToPivotNames = colsTP;
		
		ResultSetMetaData rsmd = rs.getMetaData();
		rsColsCount = rsmd.getColumnCount();
		measureCols = new ArrayList<String>();
		List<String> colsToPivotNotFound = new ArrayList<String>();
		List<String> colsNotToPivotNotFound = new ArrayList<String>();
		colsToPivotNotFound.addAll(colsToPivotNames);
		colsNotToPivotNotFound.addAll(colsNotToPivot);
		for(int i=1;i<=rsColsCount;i++) {
			String colName = rsmd.getColumnName(i);
			if(colsToPivotNotFound.contains(colName)) {
				colsToPivotNotFound.remove(colName);
			}
			else if(colsNotToPivotNotFound.contains(colName)) {
				colsNotToPivotNotFound.remove(colName);
			}
			else {
			//if(!colsNotToPivot.contains(colName) && !colsToPivotNames.contains(colName)) {
				measureCols.add(colName);
				valuesForEachMeasure.add(new HashMap<String, String>());
			}
			log.debug("colName "+i+" [colCount="+rsColsCount+"]: "+colName);
		}
		
		if(colsToPivotNotFound.size()>0) {
			throw new RuntimeException("cols to pivot not found: "+colsToPivotNotFound);
		}
		if(colsNotToPivotNotFound.size()>0) {
			throw new RuntimeException("cols not to pivot not found: "+colsNotToPivotNotFound);
		}
		
		metadata = new RSMetaDataAdapter(null, null, newColNames);
		
		if(doProcess) { process(); }
	}
	
	//XXX: addObservers, ...
	
	/*
	 * after process, resultset is ready to be used... if any other method is called before,
	 * process is called from it...
	 */
	public void process() throws SQLException {
		int count = 0;
		String lastColsNotToPivotKey = "";
		
		while(rs.next()) {
			String key = getKey();
			for(int i=0;i<measureCols.size();i++) {
				String measureCol = measureCols.get(i);
				Map<String, String> values = valuesForEachMeasure.get(i);
				String value = rs.getString(measureCol);
				
				//TODO: if value already existed, throw exception? aggregate?
				String prevValue = values.get(key);
				if(prevValue!=null) {
					log.warn("prevValue not null[measurecol="+measureCol+";key="+key+"]: "+prevValue);
				}
				
				values.put(key, value);
				log.debug("put: key="+key+" ; val="+value);
			}
			
			String colsNotToPivotKey = key.substring(0, key.indexOf("%"));
			if(!colsNotToPivotKey.equals(lastColsNotToPivotKey)) {
				//new row!
				rowCount++;
				nonPivotKeyValues.add(colsNotToPivotKey);
				/*List<String> nonPivotRowVals = new ArrayList<String>();
				for(String s: colsNotToPivot) {
					nonPivotRowVals.add(e)
				}
				//nonPivotValues.*/
				lastColsNotToPivotKey = colsNotToPivotKey;
			}
			
			log.debug("key: "+key);
			
			count++;
			
			//XXX: observer: count%COUNT_SIZE==0 ...
			
			if(count%logEachXRows == 0) {
				log.debug("processed row count: "+count);
			}
		}

		originalRSRowCount = count;
		processed = true;
		
		processMetadata();
	}
	
	public void processMetadata() {
		newColNames.clear();
		
		//-- create rs-metadata --
		//create non pivoted col names
		for(int i=0;i<colsNotToPivot.size();i++) {
			String col = colsNotToPivot.get(i);
			newColNames.add(col);
		}
		
		//foreach pivoted column
		genNewCols(0, "");
		
		//single-measure
		if(measureCols.size()==1 && alwaysShowMeasures) {
			if(showMeasuresFirst) {
				for(int i=colsNotToPivot.size();i<newColNames.size();i++) {
					newColNames.set(i, measureCols.get(0)+"|"+newColNames.get(i));
				}
			}
			else {
				for(int i=colsNotToPivot.size();i<newColNames.size();i++) {
					newColNames.set(i, newColNames.get(i)+"|"+measureCols.get(0));
				}
			}
		}
		//multi-measure
		else {
			//TODO: multi-measure
		}
	}
	
	void genNewCols(int colNumber, String partialColName) {
		String colName = colsToPivotNames.get(colNumber);
		Set<String> colVals = keyColValues.get(colName);
		for(String v: colVals) {
			String colFullName = partialColName+(colNumber==0?"":COLS_SEP)+colName+COLVAL_SEP+v;
			if(colNumber+1==colsToPivotNames.size()) {
				//add col name
				log.debug("col-full-name: "+colFullName);
				newColNames.add(colFullName);
			}
			else {
				log.debug("col-partial-name: "+colFullName);
				genNewCols(colNumber+1, colFullName);
			}
		}
	}
	
	String getKey() throws SQLException {
		StringBuilder sb = new StringBuilder();
		for(int i=0;i<colsNotToPivot.size();i++) {
			String col = colsNotToPivot.get(i);
			String val = rs.getString(col);
			if(val==null) { log.warn("value for key col is null [col = "+col+"]"); }
			addValueToSet(col, val);
			sb.append( (i==0?"":"|") + val);
		}
		sb.append("%");
		for(int i=0;i<colsToPivotNames.size();i++) {
			String col = colsToPivotNames.get(i);
			String val = rs.getString(col);
			if(val==null) { log.warn("value for pivotted key col is null [col = "+col+"]"); }
			addValueToSet(col, val);
			sb.append( (i==0?"":"|") + val);
		}
		/*sb.append("%");
		for(String col: measureCols) {
			sb.append(col+"|");
		}*/
		return sb.toString();
	}
	
	void addValueToSet(String col, String val) {
		//adds value to set
		Set<String> vals = keyColValues.get(col);
		if(vals==null) {
			//XXX: order of elements inside set: use Comparator/Comparable
			vals = new TreeSet<String>();
			keyColValues.put(col, vals);
		}
		vals.add(val);
	}
	
	//=============== util methods =============
	static Map<String, Comparable> list2Map(List<String> colsToPivot) {
		final Map<String, Comparable> colsToPivotMap = new LinkedHashMap<String, Comparable>();
		for(String s: colsToPivot) {
			colsToPivotMap.put(s, null);
		}
		return colsToPivotMap;
	}
	//=============== RS methods ===============
	
	@Override
	public ResultSetMetaData getMetaData() throws SQLException {
		return metadata;
	}
	
	@Override
	public int getType() throws SQLException {
		return ResultSet.TYPE_SCROLL_SENSITIVE;
	}
	
	@Override
	public void beforeFirst() throws SQLException {
		resetPosition();
	}
	
	@Override
	public boolean first() throws SQLException {
		resetPosition();
		return next();
	}
	
	@Override
	public boolean absolute(int row) throws SQLException {
		if(rowCount>=row) {
			position = row-1;
			updateCurrentElement();
			return true;
		}
		return false;
	}
	
	@Override
	public boolean relative(int rows) throws SQLException {
		int newpos = position + rows + 1;
		if(newpos>0) { return absolute(newpos); }
		return false;
	}

	@Override
	public boolean next() throws SQLException {
		if(rowCount-1 > position) {
			position++;
			updateCurrentElement();
			return true;
		}
		return false;
	}
	
	void updateCurrentElement() {
		if(position<0) {
			currentNonPivotKey = null;
		}
		else {
			currentNonPivotKey = nonPivotKeyValues.get(position);
		}
	}
	
	void resetPosition() {
		position = -1;
		updateCurrentElement();
	}
	
	@Override
	public String getString(String columnLabel) throws SQLException {
		int index = colsNotToPivot.indexOf(columnLabel);
		log.debug("getString:: "+columnLabel+"/"+index+" :"+colsNotToPivot+":cnpk="+currentNonPivotKey+":"+Arrays.asList(currentNonPivotKey.split("\\|")));
		if(index>=0) {
			//is nonpivotcol
			return currentNonPivotKey.split("\\|")[index];
		}
		
		index = newColNames.indexOf(columnLabel);
		if(index>=0) {
			//is pivotcol
			
			int measureIndex = -1; 

			StringBuilder sb = new StringBuilder();
			String[] parts = columnLabel.split(COLS_SEP_PATTERN);
			for(int i=0;i<parts.length;i++) {
				String p = parts[i];
				int measureColsIndex = measureCols.indexOf(p);
				if(measureColsIndex>=0) {
					if(measureIndex>=0) {
						log.warn("more than 1 measure name in column name? label: "+columnLabel);
					}
					measureIndex = measureColsIndex;
				}
				else {
					String[] colAndVal = p.split(COLVAL_SEP_PATTERN);
					sb.append( (sb.length()==0?"":"|") + colAndVal[1]);
				}
			}
			String key = currentNonPivotKey+"%"+sb.toString();
			log.debug("pivotCol: key:: "+key);
			
			//single measure
			if(measureIndex==-1) { measureIndex = 0; }
			return valuesForEachMeasure.get(measureIndex).get(key);
			
			//XXX multi-measure ?
		}
		throw new SQLException("unknown column: '"+columnLabel+"'");
	}

	@Override
	public String getString(int columnIndex) throws SQLException {
		log.debug("index: "+columnIndex+" // "+colsNotToPivot+":"+newColNames);
		if(columnIndex <= colsNotToPivot.size()) {
			return getString(colsNotToPivot.get(columnIndex-1));
		}
		if(columnIndex - colsNotToPivot.size() <= newColNames.size()) {
			return getString(newColNames.get(columnIndex-1));
		}
		throw new SQLException("unknown column index: "+columnIndex);
	}
	
	@Override
	public Object getObject(String columnLabel) throws SQLException {
		return getString(columnLabel);
	}

	@Override
	public Object getObject(int columnIndex) throws SQLException {
		return getString(columnIndex);
	}
}
