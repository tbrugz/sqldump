package tbrugz.sqldump.resultset.pivot;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.regex.Pattern;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import tbrugz.sqldump.resultset.AbstractResultSet;
import tbrugz.sqldump.resultset.RSMetaDataTypedAdapter;

/*
 * TODO: after PivotRS is built, add possibility for change key-col <-> key-row (pivotting) - processMetadata()!
 * TODOne: multiple column type (beside String): Integer, Double, Date
 * XXX: allow null key (pivotted) values (add StringComparatorNullFirst/Last ?)
 * XXX: option to remove cols/rows/both where all measures are null
 * XXXxx: aggregate if duplicated key found? first(), last()?
 * TODOne: option to show measures in columns
 * - MeasureNames is a dimension (key)
 * XXX: implement olap4j's CellSet? maybe subclass should...
 */
@SuppressWarnings("rawtypes")
public class PivotResultSet extends AbstractResultSet {
	
	static final Log log = LogFactory.getLog(PivotResultSet.class);
	
	public enum Aggregator {
		FIRST,
		LAST
		//COUNT?
		//numeric: AVG, MAX, MIN, SUM, ...
	}

	static final String COLS_SEP = "|";
	static final String COLS_SEP_PATTERN = Pattern.quote(COLS_SEP);
	static final String COLVAL_SEP = ":";
	static final String COLVAL_SEP_PATTERN = Pattern.quote(COLVAL_SEP);

	static final int logEachXRows = 1000;

	static final Aggregator DEFAULT_AGGREGATOR = Aggregator.LAST;
	
	static final String MEASURES_COLNAME = "Measure"; 
	
	public static final int SHOW_MEASURES_IN_ROWS = 0x01;
	public static final int SHOW_MEASURES_LAST = 0x02;
	public static final int SHOW_MEASURES_ALLWAYS = 0x04;
	
	// original ResultSet properties
	final ResultSet rs;
	final int rsColsCount;
	boolean processed = false;
	int originalRSRowCount;
	public static Aggregator aggregator = DEFAULT_AGGREGATOR; //used in process(), refactor?
	
	// pivot "Constructor" properties
	final List<String> colsNotToPivot;
	final Map<String, Comparable> colsToPivot;
	final List<String> measureCols = new ArrayList<String>();
	final List<Integer> colsNotToPivotType = new ArrayList<Integer>();
	//final List<Integer> colsNotToPivotIndex = new ArrayList<Integer>();
	final List<Integer> measureColsType = new ArrayList<Integer>();
	final transient List<String> colsToPivotNames; // derived from colsToPivot
	
	// data properties - set in processMetadata()?
	final Map<String, Set<Object>> keyColValues = new HashMap<String, Set<Object>>();

	// data properties - set in process()
	final List<Key> nonPivotKeyValues = new ArrayList<Key>();
	final List<Map<Key, Object>> valuesForEachMeasure = new ArrayList<Map<Key, Object>>(); //new HashMap<String, String>();
	
	// pivot metadata properties
	final List<String> newColNames = new ArrayList<String>();
	final List<Integer> newColTypes = new ArrayList<Integer>();
	final ResultSetMetaData metadata;
	
	// pivot ResultSet properties
	int position = -1;
	int rowCount = 0;
	Key currentNonPivotKey = null;
	boolean showMeasuresInColumns = true;
	boolean showMeasuresFirst = true;
	boolean alwaysShowMeasures = false;

	// colsNotToPivot+colsToPivot - key cols ?
	
	public PivotResultSet(ResultSet rs, List<String> colsNotToPivot, List<String> colsToPivot) throws SQLException {
		this(rs, colsNotToPivot, colsToPivot, true);
	}

	public PivotResultSet(ResultSet rs, List<String> colsNotToPivot, List<String> colsToPivot, boolean doProcess) throws SQLException {
		this(rs, colsNotToPivot, list2Map(colsToPivot), doProcess, 0);
	}
	
	public PivotResultSet(ResultSet rs, List<String> colsNotToPivot, Map<String, Comparable> colsToPivot,
			boolean doProcess) throws SQLException {
		this(rs, colsNotToPivot, colsToPivot, doProcess, 0);
	}
	
	public PivotResultSet(ResultSet rs, List<String> colsNotToPivot, Map<String, Comparable> colsToPivot,
			boolean doProcess, int flags) throws SQLException {
		this.rs = rs;
		this.colsNotToPivot = colsNotToPivot;
		this.colsToPivot = colsToPivot;
		setFlags(flags);
		
		for(int i=0;i<colsNotToPivot.size();i++) {
			this.colsNotToPivotType.add(null);
			//this.colsNotToPivotIndex.add(null);
		}
		
		colsToPivotNames = new ArrayList<String>();
		for(String key: colsToPivot.keySet()) {
			colsToPivotNames.add(key);
		}
		
		ResultSetMetaData rsmd = rs.getMetaData();
		rsColsCount = rsmd.getColumnCount();
		
		List<String> rsColNames = new ArrayList<String>();
		
		List<String> colsToPivotNotFound = new ArrayList<String>();
		List<String> colsNotToPivotNotFound = new ArrayList<String>();
		colsToPivotNotFound.addAll(colsToPivotNames);
		colsNotToPivotNotFound.addAll(colsNotToPivot);
		for(int i=1;i<=rsColsCount;i++) {
			String colName = rsmd.getColumnName(i);
			rsColNames.add(colName);

			int index = colsNotToPivot.indexOf(colName);
			if(index>=0) {
				//colsNotToPivotType.set(index, Types.VARCHAR); //XXXxx set non-pivot column type
				int type = rsmd.getColumnType(i);
				colsNotToPivotType.set(index, type);
				//colsNotToPivotIndex.set(index, i);
			}
			
			if(colsToPivotNotFound.contains(colName)) {
				colsToPivotNotFound.remove(colName);
			}
			else if(colsNotToPivotNotFound.contains(colName)) {
				colsNotToPivotNotFound.remove(colName);
			}
			else {
			//if(!colsNotToPivot.contains(colName) && !colsToPivotNames.contains(colName)) {
				measureCols.add(colName);
				measureColsType.add(rsmd.getColumnType(i));
				valuesForEachMeasure.add(new HashMap<Key, Object>());
			}
			//log.debug("orig colName ["+i+"/"+rsColsCount+"]: "+colName);
		}
		
		if(colsToPivotNotFound.size()>0) {
			throw new RuntimeException("cols to pivot not found: "+colsToPivotNotFound);
		}
		if(colsNotToPivotNotFound.size()>0) {
			throw new RuntimeException("cols not to pivot not found: "+colsNotToPivotNotFound);
		}
		
		metadata = new RSMetaDataTypedAdapter(null, null, newColNames, newColTypes);
		
		log.debug("rsColNames="+rsColNames);
		log.debug("colsNotToPivot: names="+this.colsNotToPivot+" ; types="+colsNotToPivotType);
		
		if(doProcess) { process(); }
	}
	
	void setFlags(int flags) {
		//log.debug("flags: "+flags);
		alwaysShowMeasures = (flags & SHOW_MEASURES_ALLWAYS) != 0;
		showMeasuresFirst = (flags & SHOW_MEASURES_LAST) == 0;
		showMeasuresInColumns = (flags & SHOW_MEASURES_IN_ROWS) == 0;
	}
	
	//XXX: addObservers, ...
	
	/*
	 * after process, resultset is ready to be used... if any other method is called before,
	 * process is called from it...
	 */
	public void process() throws SQLException {
		int count = 0;
		Set<Key> colsNotToPivotKeySet = new HashSet<Key>();
		
		while(rs.next()) {
			Object[] keyArr = getKeyArray(rs);
			Key key = new Key(keyArr);
			for(int i=0;i<measureCols.size();i++) {
				String measureCol = measureCols.get(i);
				Map<Key, Object> values = valuesForEachMeasure.get(i);
				Object value = rs.getObject(measureCol);
				
				Object prevValue = values.get(key);
				if(prevValue==null) {
					values.put(key, value);
				}
				else {
					log.warn("prevValue not null[measurecol="+measureCol+";key="+key+";aggr="+aggregator+"]: "+prevValue);
					switch (aggregator) {
					case FIRST:
						//do nothing
						break;
					case LAST:
						values.put(key, value);
						break;
					default:
						throw new IllegalStateException("unknown aggregator: "+aggregator);
					} 
				}
				
				//log.debug("put: key="+key+" ; val="+value+"; aggr="+aggregator);
			}
			
			if(showMeasuresInColumns) {
				Key keyNotToPivotArr = new Key(Arrays.copyOf(keyArr, colsNotToPivot.size())); 
				//log.info("new row? "+keyNotToPivotArr);
				if(!colsNotToPivotKeySet.contains(keyNotToPivotArr)) {
					//new row!
					rowCount++;
					nonPivotKeyValues.add(keyNotToPivotArr);
					colsNotToPivotKeySet.add(keyNotToPivotArr);
				}
			}
			else {
				Object[] keyVals = new Object[colsNotToPivot.size()+1];
				System.arraycopy(keyArr, 0, keyVals, 0+(showMeasuresFirst?1:0), colsNotToPivot.size());
				if(showMeasuresFirst) {
					keyVals[0] = measureCols.get(0); //just put first measure (for now)
				}
				else {
					keyVals[keyVals.length-1] = measureCols.get((rowCount+1)%measureCols.size()); 
				}
				Key keyNotToPicotArr = new Key(keyVals);
				if(!colsNotToPivotKeySet.contains(keyNotToPicotArr)) {
					//new row!
					rowCount++;
					nonPivotKeyValues.add(keyNotToPicotArr);
					colsNotToPivotKeySet.add(keyNotToPicotArr);
				}
			}
			
			//log.debug("key: "+key);
			
			count++;
			
			//XXX: observer: count%COUNT_SIZE==0 ...
			
			if(count%logEachXRows == 0) {
				log.debug("processed row count: "+count);
			}
		}
		
		//log.info("before[mic="+showMeasuresInColumns+";smf="+showMeasuresFirst+"]: "+nonPivotKeyValues);

		if(!showMeasuresInColumns) {
			//populate measures in column
			if(showMeasuresFirst) {
				int origKeysLen = nonPivotKeyValues.size();
				for(int j=1;j<measureCols.size();j++) {
					for(int i=0;i<origKeysLen;i++) {
						Key key = nonPivotKeyValues.get(i);
						Object[] values = Arrays.copyOf(key.values, key.values.length);
						values[0] = measureCols.get(j);
						nonPivotKeyValues.add(new Key(values));
						rowCount++;
					}
				}
			}
			else {
				//XXX populate nonPivotKeyValues when showMeasuresFirst is false. really?
			}
		}

		//log.info("after: "+nonPivotKeyValues);
		
		originalRSRowCount = count;
		processed = true;
		
		//log.info("valuesForEachMeasure: "+valuesForEachMeasure);
		
		processMetadata();
	}
	
	public void processMetadata() {
		//-- create rs-metadata --
		newColNames.clear();
		newColTypes.clear();
		
		log.debug("processMetadata: measures="+measureCols);
		
		//measures in columns first
		if((!showMeasuresInColumns) && showMeasuresFirst) {
			newColNames.add(MEASURES_COLNAME);
			newColTypes.add(Types.VARCHAR);
		}
		
		//create non pivoted col names
		for(int i=0;i<colsNotToPivot.size();i++) {
			String col = colsNotToPivot.get(i);
			newColNames.add(col);
			//newColTypes.add(Types.VARCHAR); //XXXxx set non-pivot column type
			newColTypes.add(colsNotToPivotType.get(i));
		}

		//measures in columns last
		if((!showMeasuresInColumns) && (!showMeasuresFirst)) {
			newColNames.add(MEASURES_COLNAME);
			newColTypes.add(Types.VARCHAR);
		}
		
		List<String> dataColumns = new ArrayList<String>();
		//foreach pivoted column
		genNewCols(0, "", dataColumns);
		
		//single-measure
		if(measureCols.size()==1 || !showMeasuresInColumns) {
			newColNames.addAll(dataColumns);
			for(int i=0;i<dataColumns.size();i++) {
				newColTypes.add(measureColsType.get(0));
			}
			if(alwaysShowMeasures) {
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
		}
		//multi-measure
		else {
			//TODOne: multi-measure
			List<String> colNames = new ArrayList<String>();
			colNames.addAll(newColNames);
			
			if(showMeasuresFirst) {
				for(int j=0;j<measureCols.size();j++) {
					String measure = measureCols.get(j);
					Integer measureColType = measureColsType.get(j);
					for(int i=0;i<dataColumns.size();i++) {
						newColNames.add(measure+"|"+dataColumns.get(i));
						newColTypes.add(measureColType);
					}
				}
			}
			else {
				for(int i=0;i<dataColumns.size();i++) {
					for(int j=0;j<measureCols.size();j++) {
						String measure = measureCols.get(j);
						newColNames.add(dataColumns.get(i)+"|"+measure);
						newColTypes.add(measureColsType.get(j));
					}
				}
			}
		}
		
		resetPosition();

		log.debug("processMetadata: columns="+newColNames+" types="+newColTypes);
	}
	
	void genNewCols(int colNumber, String partialColName, List<String> newColumns) {
		int colsToPivotCount = colsToPivotNames.size();
		String colName = colsToPivotNames.get(colNumber);
		Set<Object> colVals = keyColValues.get(colName);
		for(Object v: colVals) {
			String colFullName = partialColName+(colNumber==0?"":COLS_SEP)+colName+COLVAL_SEP+v;
			if(colNumber+1==colsToPivotCount) {
				//add col name
				//log.debug("genNewCols: col-full-name: "+colFullName);
				newColumns.add(colFullName);
			}
			else {
				//log.debug("col-partial-name: "+colFullName);
				genNewCols(colNumber+1, colFullName, newColumns);
			}
		}
	}
	
	Object[] getKeyArray(ResultSet rs) throws SQLException {
		Object[] ret = new Object[colsNotToPivot.size()+colsToPivotNames.size()];
		for(int i=0;i<colsNotToPivot.size();i++) {
			String col = colsNotToPivot.get(i);
			Object val = rs.getObject(col);
			if(val==null) { log.warn("value for key col is null [col = "+col+"]"); }
			addValueToSet(col, val);
			ret[i] = val;
		}
		for(int i=0;i<colsToPivotNames.size();i++) {
			String col = colsToPivotNames.get(i);
			Object val = rs.getObject(col);
			if(val==null) { log.warn("value for pivotted key col is null [col = "+col+"]"); }
			addValueToSet(col, val);
			ret[colsNotToPivot.size()+i] = val;
		}
		return ret;
	}
	
	void addValueToSet(String col, Object val) {
		//adds value to set
		Set<Object> vals = keyColValues.get(col);
		if(vals==null) {
			//XXX: order of elements inside set: use Comparator/Comparable
			vals = new TreeSet<Object>();
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
			return;
		}
		currentNonPivotKey = nonPivotKeyValues.get(position);
	}
	
	void resetPosition() {
		position = -1;
		updateCurrentElement();
	}
	
	Object findPivotKeyValueFromStringValue(String colName, String value) {
		Set<Object> colVals = keyColValues.get(colName);
		for(Object o: colVals) {
			if(value.equals(o.toString())) {
				return o;
			}
		}
		return null;
	}
	
	@Override
	public Object getObject(String columnLabel) throws SQLException {
		//log.info("getObject: "+columnLabel);
		int index = colsNotToPivot.indexOf(columnLabel);
		if(index>=0) {
			//is nonpivotcol
			
			//log.debug("getObject[non-pivot]: "+columnLabel+" [nonPivotKey:"+index+"] :"+colsNotToPivot+":cnpk="+currentNonPivotKey+":"+java.util.Arrays.asList(currentNonPivotKey.split("\\|")));
			//log.debug("getObject[non-pivot]: "+columnLabel+" [nonPivotKey:"+index+"] : "+currentNonPivotKey.values[index]);
			//XXXdone: return non-string values for non-pivot columns?
			if(!showMeasuresInColumns && showMeasuresFirst) {
				return currentNonPivotKey.values[index+1];
			}
			return currentNonPivotKey.values[index];
		}
		
		// measures column?
		if((!showMeasuresInColumns) && MEASURES_COLNAME.equals(columnLabel)) {
			if(showMeasuresFirst) {
				return measureCols.get(position/measureCols.size());
			}
			else {
				return measureCols.get(position%measureCols.size());
			}
		}
			
		index = newColNames.indexOf(columnLabel);
		if(index>=0) {
			//is pivotcol
			
			int measureIndex = -1; 
			List<Object> sb = new ArrayList<Object>();
			String[] parts = columnLabel.split(COLS_SEP_PATTERN);
			//log.debug("getObject: parts: "+Arrays.asList(parts));
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
					Object value = findPivotKeyValueFromStringValue(colAndVal[0], colAndVal[1]);
					sb.add(value);
				}
			}
			Object[] keyArr = new Object[currentNonPivotKey.values.length+sb.size()];
			for(int i=0;i<currentNonPivotKey.values.length;i++) {
				keyArr[i] = currentNonPivotKey.values[i];
			}
			for(int i=0;i<sb.size();i++) {
				keyArr[i+currentNonPivotKey.values.length] = sb.get(i);
			}
			if(!showMeasuresInColumns) {
				//remove measure name from key
				if(showMeasuresFirst) {
					keyArr = Arrays.copyOfRange(keyArr, 1, keyArr.length);
				}
				else {
					int npkc = currentNonPivotKey.values.length-1;
					Object[] newKeyArr = new Object[keyArr.length-1];
					System.arraycopy(keyArr, 0, newKeyArr, 0, npkc);
					//log.debug("[int;"+npkc+"] before: "+Arrays.asList(keyArr)+" ; after: "+Arrays.asList(newKeyArr));
					System.arraycopy(keyArr, npkc+1, newKeyArr, npkc, keyArr.length-npkc-1);
					//log.debug("getObject: measureKey: before: "+Arrays.asList(keyArr)+" ; after: "+Arrays.asList(newKeyArr));
					keyArr = newKeyArr;
				}
			}
			Key key = new Key(keyArr);
			//log.debug("pivotCol: key:: "+key);
			
			if(measureIndex==-1) {
				//measure name in its own column
				if((!showMeasuresInColumns)) {
					if(showMeasuresFirst) {
						measureIndex = position/measureCols.size();
					}
					else {
						measureIndex = position%measureCols.size();
					}
				}
				//single measure
				else {
					measureIndex = 0;
				}
			}
			Map<Key, Object> measureMap = valuesForEachMeasure.get(measureIndex);
			//log.debug("getObject[pivot] value [key="+key+",measureIndex="+measureIndex+"]: "+measureMap.get(key));
			return measureMap.get(key);
		}
		throw new SQLException("unknown column: '"+columnLabel+"'");
	}

	@Override
	public Object getObject(int columnIndex) throws SQLException {
		//log.debug("getObject[by-index]: "+columnIndex+" // "+colsNotToPivot+":"+newColNames);
		if((!showMeasuresInColumns)) {
			if(showMeasuresFirst && columnIndex==1) {
				return getObject(MEASURES_COLNAME);
			}
			if((!showMeasuresFirst) && columnIndex==colsNotToPivot.size()+1) {
				return getObject(MEASURES_COLNAME);
			}
		}
		if(columnIndex <= colsNotToPivot.size()) {
			return getObject(colsNotToPivot.get(columnIndex-1));
		}
		if(columnIndex - colsNotToPivot.size() <= newColNames.size()) {
			return getObject(newColNames.get(columnIndex-1));
		}
		throw new SQLException("unknown column index: "+columnIndex);
	}
	
	@Override
	public String getString(String columnLabel) throws SQLException {
		Object o = getObject(columnLabel);
		if(o==null) { return null; }
		return String.valueOf(o);
	}

	@Override
	public String getString(int columnIndex) throws SQLException {
		Object o = getObject(columnIndex);
		if(o==null) { return null; }
		return String.valueOf(o);
	}
	
	static double getDoubleValue(Object o) {
		if(o==null) { return 0d; }
		if(o instanceof Number) {
			return ((Number)o).doubleValue();
		}
		return getDoubleValue(o.toString());
	}

	static long getLongValue(Object o) {
		if(o==null) { return 0l; }
		if(o instanceof Number) {
			return ((Number)o).longValue();
		}
		return (long) getDoubleValue(o.toString());
	}
	
	static int getIntValue(Object o) {
		if(o==null) { return 0; }
		if(o instanceof Number) {
			return ((Number)o).intValue();
		}
		return (int) getDoubleValue(o.toString());
	}

	static double getDoubleValue(String s) {
		try {
			return Double.parseDouble(s);
		}
		catch(NumberFormatException e) {
			return 0d;
		}
	}
	
	static Date getDate(Object o) {
		if(o==null) { return null; }
		//log.debug("getDate: "+o+" ; "+o.getClass());
		if(o instanceof Date) {
			return (Date) o;
		}
		return null;
	}
	
	@Override
	public double getDouble(String columnLabel) throws SQLException {
		Object o = getObject(columnLabel);
		return getDoubleValue(o);
	}
	
	@Override
	public double getDouble(int columnIndex) throws SQLException {
		Object o = getObject(columnIndex);
		return getDoubleValue(o);
	}
	
	@Override
	public long getLong(String columnLabel) throws SQLException {
		Object o = getObject(columnLabel);
		return getLongValue(o);
	}
	
	@Override
	public long getLong(int columnIndex) throws SQLException {
		Object o = getObject(columnIndex);
		return getLongValue(o);
	}
	
	@Override
	public int getInt(String columnLabel) throws SQLException {
		Object o = getObject(columnLabel);
		return getIntValue(o);
	}
	
	@Override
	public int getInt(int columnIndex) throws SQLException {
		Object o = getObject(columnIndex);
		return getIntValue(o);
	}
	
	//public short getShort(int columnIndex) throws SQLException;
	
	//public byte getByte(int columnIndex) throws SQLException;
	
	//public float getFloat(int columnIndex) throws SQLException;
	
	@Override
	public java.sql.Date getDate(int columnIndex) throws SQLException {
		Object o = getObject(columnIndex);
		Date d = getDate(o);
		if(d==null) { return null; }
		return new java.sql.Date( d.getTime() );
	}

	@Override
	public Timestamp getTimestamp(int columnIndex) throws SQLException {
		Object o = getObject(columnIndex);
		Date d = getDate(o);
		if(d==null) { return null; }
		return new Timestamp( d.getTime() );
	}
	
}
