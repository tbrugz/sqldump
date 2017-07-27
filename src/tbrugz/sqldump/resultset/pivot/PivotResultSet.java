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
 * XXX: option to remove cols/rows/both where all measures are null - MDX: non empty ;)
 * XXXxx: aggregate if duplicated key found? first(), last()?
 * TODOne: option to show measures in columns
 * - MeasureNames is a dimension (key)
 * XXX: implement olap4j's CellSet? maybe subclass should...
 * XXX: how to order rows? & cols?
 */
@SuppressWarnings("rawtypes")
public class PivotResultSet extends AbstractResultSet {
	
	static final Log log = LogFactory.getLog(PivotResultSet.class);
	
	public enum Aggregator {
		FIRST,
		LAST
		//COUNT? DISTINCT-COUNT?
		//numeric: AVG, MAX, MIN, SUM, ...
	}

	public static final String COLS_SEP = "|||";
	public static final String COLS_SEP_PATTERN = Pattern.quote(COLS_SEP);
	public static final String COLVAL_SEP = ":::";
	public static final String COLVAL_SEP_PATTERN = Pattern.quote(COLVAL_SEP);

	static final int logEachXRows = 1000;

	static final Aggregator DEFAULT_AGGREGATOR = Aggregator.LAST;
	
	static final String MEASURES_COLNAME = "Measure"; 
	
	public static final int SHOW_MEASURES_IN_ROWS = 0x01;
	public static final int SHOW_MEASURES_LAST = 0x02;
	public static final int SHOW_MEASURES_ALLWAYS = 0x04;
	public static final int SHOW_EMPTY_COLS = 0x08;
	public static final int FLAG_SORT_NONPIVOT_KEYS = 0x16;
	
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
	//int rowCount = 0;
	Key currentNonPivotKey = null;
	boolean showMeasuresInColumns = true;
	boolean showMeasuresFirst = true;
	boolean alwaysShowMeasures = false;
	boolean ignoreNullValues = true; // true: may use less memory...
	boolean noColsWithNullValues = false; // non empty (mdx)?
	boolean sortNonPivotKeyValues = true;

	// colsNotToPivot+colsToPivot - key cols ?
	
	public PivotResultSet(ResultSet rs, List<String> colsNotToPivot, List<String> colsToPivot) throws SQLException {
		this(rs, colsNotToPivot, colsToPivot, true);
	}

	public PivotResultSet(ResultSet rs, List<String> colsNotToPivot, List<String> colsToPivot, boolean doProcess) throws SQLException {
		this(rs, colsNotToPivot, list2Map(colsToPivot), doProcess, 0);
	}
	
	public PivotResultSet(ResultSet rs, List<String> colsNotToPivot, List<String> colsToPivot, boolean doProcess, int flags) throws SQLException {
		this(rs, colsNotToPivot, list2Map(colsToPivot), doProcess, flags);
	}
	
	public PivotResultSet(ResultSet rs, List<String> colsNotToPivot, Map<String, Comparable> colsToPivot, boolean doProcess) throws SQLException {
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
		if(colsToPivot!=null) {
			for(String key: colsToPivot.keySet()) {
				colsToPivotNames.add(key);
			}
		}
		/*if(colsToPivotNames.size()<1) {
			//allow no pivot cols: only measures on cols...
			throw new IllegalArgumentException("no columns selected to pivot");
		}*/
		
		ResultSetMetaData rsmd = rs.getMetaData();
		rsColsCount = rsmd.getColumnCount();
		
		List<String> rsColNames = new ArrayList<String>();
		
		List<String> colsToPivotNotFound = new ArrayList<String>();
		List<String> colsNotToPivotNotFound = new ArrayList<String>();
		colsToPivotNotFound.addAll(colsToPivotNames);
		colsNotToPivotNotFound.addAll(colsNotToPivot);
		for(int i=1;i<=rsColsCount;i++) {
			String colName = rsmd.getColumnLabel(i);
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
		showMeasuresInColumns = (flags & SHOW_MEASURES_IN_ROWS) == 0;
		showMeasuresFirst = (flags & SHOW_MEASURES_LAST) == 0;
		alwaysShowMeasures = (flags & SHOW_MEASURES_ALLWAYS) != 0;
		noColsWithNullValues = (flags & SHOW_EMPTY_COLS) != 0;
		sortNonPivotKeyValues = (flags & FLAG_SORT_NONPIVOT_KEYS) != 0;
		//log.info("alwaysShowMeasures="+alwaysShowMeasures+"; showMeasuresFirst="+showMeasuresFirst+"; showMeasuresInColumns="+showMeasuresInColumns+"; noColsWithNullValues="+noColsWithNullValues+"; flags: "+flags);
	}
	
	//XXX: addObservers, ...
	
	/*
	 * after process, resultset is ready to be used... if any other method is called before,
	 * process is called from it...
	 */
	public void process() throws SQLException {
		int count = 0;
		Set<Key> colsNotToPivotKeySet = new HashSet<Key>();
		
		int countErrPrevValue = 0, countErrPrevValueEquals = 0;
		
		while(rs.next()) {
			Object[] keyArr = getKeyArray(rs);
			Key key = new Key(keyArr);
			for(int i=0;i<measureCols.size();i++) {
				String measureCol = measureCols.get(i);
				Object value = rs.getObject(measureCol);
				if(ignoreNullValues && value==null) { continue; }

				Map<Key, Object> values = valuesForEachMeasure.get(i);
				Object prevValue = values.get(key);
				if(prevValue==null) {
					values.put(key, value);
				}
				else {
					if(prevValue.equals(value)) {
						countErrPrevValueEquals++;
						log.debug("prevValue not null & equal to current value [measurecol="+measureCol+";key="+key+";aggr="+aggregator+"]: prev/new="+value);
					}
					else {
						countErrPrevValue++;
						//warn?
						log.debug("prevValue not null [measurecol="+measureCol+";key="+key+";aggr="+aggregator+"]: prev="+prevValue+" ; new="+value);
					}
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
					//rowCount++;
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
					keyVals[keyVals.length-1] = measureCols.get((getRowCount()+1)%measureCols.size()); 
				}
				Key keyNotToPicotArr = new Key(keyVals);
				if(!colsNotToPivotKeySet.contains(keyNotToPicotArr)) {
					//new row!
					//rowCount++;
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
		
		if(countErrPrevValueEquals>0 || countErrPrevValue>0) {
			log.warn("prevValue error count: countErrPrevValue=="+countErrPrevValue+" ; countErrPrevValueEquals=="+countErrPrevValueEquals+" [aggr="+aggregator+"]");
		}
		
		//log.info("before[mic="+showMeasuresInColumns+";smf="+showMeasuresFirst+"]: "+nonPivotKeyValues);

		if(!showMeasuresInColumns) {
			//log.info("1 [showMeasuresInColumns="+showMeasuresInColumns+";showMeasuresFirst="+showMeasuresFirst+"] nonPivotKeyValues[#"+nonPivotKeyValues.size()+"]="+nonPivotKeyValues);
			//populate measures in column
			if(showMeasuresFirst) {
				int origKeysLen = nonPivotKeyValues.size();
				for(int j=1;j<measureCols.size();j++) {
					for(int i=0;i<origKeysLen;i++) {
						Key key = nonPivotKeyValues.get(i);
						Object[] values = Arrays.copyOf(key.values, key.values.length);
						values[0] = measureCols.get(j);
						nonPivotKeyValues.add(new Key(values));
						//rowCount++;
					}
				}
			}
			else {
				//FIXedME populate nonPivotKeyValues when showMeasuresFirst is false. really?
				List<Key> newNonPivotKeyValues = new ArrayList<Key>();
				int origKeysLen = nonPivotKeyValues.size();
				for(int i=0;i<origKeysLen;i++) {
					for(int j=0;j<measureCols.size();j++) {
						Key key = nonPivotKeyValues.get(i);
						Object[] values = Arrays.copyOf(key.values, key.values.length);
						values[values.length-1] = measureCols.get(j);
						newNonPivotKeyValues.add(new Key(values));
						//rowCount++;
					}
				}
				nonPivotKeyValues.clear();
				nonPivotKeyValues.addAll(newNonPivotKeyValues);
				//rowCount = newNonPivotKeyValues.size();
			}
			//log.info("2 [showMeasuresInColumns="+showMeasuresInColumns+";showMeasuresFirst="+showMeasuresFirst+"] nonPivotKeyValues[#"+nonPivotKeyValues.size()+"]="+nonPivotKeyValues);
		}

		//log.info("nonPivotKeyValues: "+nonPivotKeyValues+" / "+Arrays.asList(nonPivotKeyValues.get(0).values[0])+" / "+nonPivotKeyValues.get(0).values[0].getClass());
		if(sortNonPivotKeyValues) {
			nonPivotKeyValues.sort(null);
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
			if(dataColumns.size()==0) {
				newColNames.add(measureCols.get(0));
				newColTypes.add(measureColsType.get(0));
			}
			if(alwaysShowMeasures && colsToPivotNames.size()>0) {
				if(showMeasuresFirst) {
					for(int i=colsNotToPivot.size()+(showMeasuresInColumns?0:1);i<newColNames.size();i++) {
						newColNames.set(i, measureCols.get(0)+COLS_SEP+newColNames.get(i));
					}
				}
				else {
					for(int i=colsNotToPivot.size()+(showMeasuresInColumns?0:1);i<newColNames.size();i++) {
						newColNames.set(i, newColNames.get(i)+COLS_SEP+measureCols.get(0));
					}
				}
			}
		}
		//multi-measure
		else {
			//TODOne: multi-measure
			//List<String> colNames = new ArrayList<String>();
			//colNames.addAll(newColNames);
			//log.info("processMetadata:: measureCols="+measureCols+" ; dataColumns="+dataColumns);
			
			if(showMeasuresFirst) {
				for(int j=0;j<measureCols.size();j++) {
					String measure = measureCols.get(j);
					Integer measureColType = measureColsType.get(j);
					for(int i=0;i<dataColumns.size();i++) {
						newColNames.add(measure+COLS_SEP+dataColumns.get(i));
						newColTypes.add(measureColType);
					}
					if(dataColumns.size()==0) {
						newColNames.add(measure);
						newColTypes.add(measureColType);
					}
				}
			}
			else {
				for(int i=0;i<dataColumns.size();i++) {
					for(int j=0;j<measureCols.size();j++) {
						String measure = measureCols.get(j);
						newColNames.add(dataColumns.get(i)+COLS_SEP+measure);
						newColTypes.add(measureColsType.get(j));
					}
				}
				if(dataColumns.size()==0) {
					for(int j=0;j<measureCols.size();j++) {
						String measure = measureCols.get(j);
						newColNames.add(measure);
						newColTypes.add(measureColsType.get(j));
					}
				}
			}
		}
		
		if(noColsWithNullValues) {
			removeColumnsWithNoValues();
		}
		
		resetPosition();

		log.debug("processMetadata: columns="+newColNames+" types="+newColTypes);
	}
	
	void genNewCols(int colNumber, String partialColName, List<String> newColumns) {
		int colsToPivotCount = colsToPivotNames.size();
		if(colsToPivotCount==0 && colNumber==0) {
			//log.warn("colNumber="+colNumber+" ; partialColName='"+partialColName+"' ; newColumns="+newColumns+" ; measureCols="+measureCols+" ; keyColValues="+keyColValues);
			//newColumns.addAll(measureCols);
			return;
		}
		//log.info("colNumber="+colNumber+" ; partialColName='"+partialColName+"' ; newColumns="+newColumns+" ; measureCols="+measureCols+" ; keyColValues="+keyColValues);
		
		String colName = colsToPivotNames.get(colNumber);
		Set<Object> colVals = keyColValues.get(colName);
		for(Object v: colVals) {
			String colFullName = partialColName+(colNumber==0?"":COLS_SEP)+colName+COLVAL_SEP+v;
			if(colNumber+1==colsToPivotCount) {
				//add col name
				//XXX test for noColsWithNullValues?
				newColumns.add(colFullName);
				/*if(!noColsWithNullValues || columnContainsValues(colFullName, colsNotToPivot.size()+colNumber)) {
					newColumns.add(colFullName);
				//log.info("genNewCols: col-full-name: "+colFullName);
				}
				else {
					log.info("genNewCols: no values! [col-full-name: "+colFullName+"]");
				}*/
			}
			else {
				//log.info("col-partial-name: "+colFullName);
				genNewCols(colNumber+1, colFullName, newColumns);
			}
		}
	}
	
	/*boolean columnContainsValues(String fullCollName, int pivotColIdx) {
		for(int i=0;i<measureCols.size();i++) {
			//String measureCol = measureCols.get(i);
			Map<Key, Object> values = valuesForEachMeasure.get(i);
			for(Map.Entry<Key, Object> e: values.entrySet()) {
				// key values order: colsNotToPivot, colsToPivotNames
				Object v = e.getKey().values[pivotColIdx];
				if(v!=null) {
					log.info("fullCN: "+fullCollName+"/"+pivotColIdx+" ; e.getKey(): "+e.getKey());
					return true;
				}
			}
		}

		return false;
	}*/

	void removeColumnsWithNoValues() {
		int cntpl = colsNotToPivot.size();
		//boolean[] colsToKeep = new boolean[newColNames.size()];
		//log.info("colsNotToPivot: "+colsNotToPivot+" ; colsToPivotNames: "+colsToPivotNames+" ; newColNames: "+newColNames);
		for(int i=newColNames.size()-1;i>=cntpl+(showMeasuresInColumns?0:1);i--) {
			String col = newColNames.get(i);
			String[] parts = col.split(COLS_SEP_PATTERN);
			String[] vals = new String[colsToPivotNames.size()];
			//String measure = null;
			int vidx = 0;
			for(int j=0;j<parts.length;j++) {
				String[] cv = parts[j].split(COLVAL_SEP_PATTERN);
				if(cv.length==2) {
					vals[vidx++] = cv[1];
				}
				/*else if(cv.length==1) {
					measure = cv[0];
				}*/
			}
			//log.info("col: "+col+" ; vals: "+Arrays.asList(vals));
			
			boolean match = false;
			for(int j=0;j<measureCols.size();j++) {
				//String measureCol = measureCols.get(i);
				Map<Key, Object> values = valuesForEachMeasure.get(j);
				for(Map.Entry<Key, Object> e: values.entrySet()) {
					//log.info("e.getKey(): "+e.getKey()+" ; cntpl: "+cntpl);
					//if( arrayPartialEquals(vals, 0, e.getKey().values, cntpl) ) {
					if( isArrayTail(toStringArray(e.getKey().values), vals) ) {
						match = true;
						//log.info("match: "+Arrays.asList(vals)+" // "+Arrays.asList(e.getKey().values));
					}
				}
			}
			if(!match) {
				newColNames.remove(i);
				newColTypes.remove(i);
			}
		}
		
		if(newColNames.size()==0) {
			log.warn("newColNames is empty [colsNotToPivot="+colsNotToPivot+"][colsToPivotNames="+colsToPivotNames+"]");
		}
		
		/*
		for(int i=0;i<colsToPivotNames.size();i++) {
			String pcol = colsToPivotNames.get(i);
			Set<Object> vs = keyColValues.get(pcol);
			for(int j=0;j<measureCols.size();j++) {
				//String measureCol = measureCols.get(i);
				Map<Key, Object> values = valuesForEachMeasure.get(j);
				for(Map.Entry<Key, Object> e: values.entrySet()) {
					log.info("e.getKey(): "+e.getKey()+" ; cntpl+i: "+(cntpl+i));
					if(vs.contains(e.getValue())) {
						
					}
				}
			}
		}
		*/
		
		
		/*
		cols_label:
		for(int i=0;i<colsToKeep.length;i++) {
			for(int j=0;j<measureCols.size();j++) {
				//String measureCol = measureCols.get(i);
				Map<Key, Object> values = valuesForEachMeasure.get(j);
				for(Map.Entry<Key, Object> e: values.entrySet()) {
					log.info("e.getKey(): "+e.getKey()+" ; cntpl+i: "+(cntpl+i));
					if(e.getKey().values[cntpl+i]!=null) {
						colsToKeep[i] = true;
						continue cols_label;
					}
					// key values order: colsNotToPivot, colsToPivotNames
					//Object v = e.getKey().values[pivotColIdx];
					//if(v!=null) {
					//	log.info("fullCN: "+fullCollName+"/"+pivotColIdx+" ; e.getKey(): "+e.getKey());
					//}
				}
			}
		}
		*/
		/*for(int i=colsToKeep.length-1;i>=0;i--) {
			if(! colsToKeep[i]) {
				log.info("removing column ["+i+"]: "+newColNames.get(i));
			}
		}*/
	}
	
	static String[] toStringArray(Object[] arr) {
		String[] ret = new String[arr.length];
		for(int i=0;i<ret.length;i++) {
			ret[i] = arr[i].toString();
		}
		return ret;
	}
	
	static boolean isArrayTail(String[] bigger, String[] tail) {
		//log.info("bigger: "+Arrays.asList(bigger)+" ; tail: "+Arrays.asList(tail));
		int tlen = tail.length;
		int bdiff = bigger.length-tlen;
		if(bdiff<0) { return false; }
		for(int i=tlen-1; i>=0; i--) {
			Object a1e = tail[i];
			Object a2e = bigger[i+bdiff];
			if( a1e==null && a2e==null ) { continue; }
			if( a1e==null || !a1e.equals(a2e)) {
				return false;
			}
		}
		//log.info(">> isTail!");
		return true;
	}
	
	/*static boolean arrayPartialEquals(Object[] a1, int a1idx, Object[] a2, int a2idx) {
		for(int i=a1idx;i<a1.length; i++) {
			Object a1e = a1[i];
			Object a2e = a2[i-a1idx+a2idx];
			if( a1e==null && a2e==null ) { continue; }
			if( a1e==null || !a1e.equals(a2e)) {
				return false;
			}
		}
		return true;
	}*/	
	
	Object[] getKeyArray(ResultSet rs) throws SQLException {
		Object[] ret = new Object[colsNotToPivot.size()+colsToPivotNames.size()];
		for(int i=0;i<colsNotToPivot.size();i++) {
			String col = colsNotToPivot.get(i);
			Object val = rs.getObject(col);
			validateKeyValue(col, val);
			addValueToSet(col, val);
			ret[i] = val;
		}
		for(int i=0;i<colsToPivotNames.size();i++) {
			String col = colsToPivotNames.get(i);
			Object val = rs.getObject(col);
			validateKeyValue(col, val);
			addValueToSet(col, val);
			ret[colsNotToPivot.size()+i] = val;
		}
		return ret;
	}
	
	void validateKeyValue(String col, Object val) {
		if(val==null) {
			log.warn("value for key col is null [col = "+col+"]");
		}
		else {
			if(val.toString().contains(COLS_SEP)) {
				log.warn("value for key col ["+col+"] contains COLS_SEP_PATTERN ["+COLS_SEP+"]: "+val);
			}
			if(val.toString().contains(COLVAL_SEP)) {
				log.warn("value for key col ["+col+"] contains COLVAL_SEP_PATTERN ["+COLVAL_SEP+"]: "+val);
			}
			//log.info("value for key col ["+col+"] ["+COLS_SEP+"/"+COLVAL_SEP+"]: "+val.toString());
		}
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
		if(colsToPivot==null) { return null; }
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
		if(getRowCount()>=row) {
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
		if(getRowCount()-1 > position) {
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
		if(colVals==null) {
			log.warn("null colVals for colName '"+colName+"'");
			return null;
		}
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
				return measureCols.get(position/(nonPivotKeyValues.size()/measureCols.size()) );
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
						measureIndex = position/(nonPivotKeyValues.size()/measureCols.size());
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
		//log.info("getObject[by-index]: idx="+columnIndex+" // colsNotToPivot="+colsNotToPivot+" ; newColNames="+newColNames+" ; showMeasuresInColumns="+showMeasuresInColumns+" ; showMeasuresFirst="+showMeasuresFirst);
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
		if(o==null) { return 0L; }
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
	
	public int getRowCount() {
		return nonPivotKeyValues.size(); //rowCount;
	}
	
	/*public int getNonPivotKeysCount() {
		return nonPivotKeyValues.size();
	}*/
	
	public int getOriginalRowCount() {
		return originalRSRowCount;
	}
	
}
