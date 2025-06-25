package tbrugz.sqldump.resultset.pivot;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
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
 * XXXdone: option to remove cols/rows/both where all measures are null - MDX: non empty ;)
 * XXX??: option to remove rows/both where all measures are null
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
	
	static class NullOkComparator implements Comparator<Object> {

		final int returnLastIsNull;
		final int returnFistIsNull;
		
		public NullOkComparator() {
			this(false);
		}

		public NullOkComparator(boolean nullsFirst) {
			returnLastIsNull = nullsFirst ?  1 : -1;
			returnFistIsNull = nullsFirst ? -1 :  1;
		}
		
		@SuppressWarnings("unchecked")
		@Override
		public int compare(Object o1, Object o2) {
			// nulls last?
			if(o1!=null && o2==null) { return returnLastIsNull; }
			if(o1==null && o2!=null) { return returnFistIsNull; }
			if(o1==null && o2==null) { return 0; }
			
			if(o1 instanceof Comparable && o2 instanceof Comparable) {
				return ((Comparable)o1).compareTo(((Comparable)o2));
			}
			if(o1 instanceof Number) {
				Number n = (Number) o1;
				Number on = (Number) o2;
				return on.intValue()-n.intValue();
			}
			return String.valueOf(o1).compareTo(String.valueOf(o2));
		}
		
	}

	public static final String COLS_SEP = "|||";
	public static final String COLS_SEP_PATTERN = Pattern.quote(COLS_SEP);
	public static final String COLVAL_SEP = ":::";
	public static final String COLVAL_SEP_PATTERN = Pattern.quote(COLVAL_SEP);
	public static final String NULL_PLACEHOLDER = "-N-U-L-L-"; // "null"; html: &#9216;

	static final int logEachXRows = 1000;

	static final Aggregator DEFAULT_AGGREGATOR = Aggregator.LAST;
	
	static final String MEASURES_COLNAME = "Measure"; 
	
	public static final int SHOW_MEASURES_IN_ROWS = 0x01;
	public static final int SHOW_MEASURES_LAST = 0x02;
	public static final int SHOW_MEASURES_ALLWAYS = 0x04;
	public static final int FLAG_NON_EMPTY_COLS = 0x08;
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
	static final NullOkComparator nullOkComparator = new NullOkComparator();
	
	// data properties - set in processMetadata()?
	final Map<String, Set<Object>> keyColValues = new HashMap<String, Set<Object>>();

	// data properties - set in process()
	final List<Key> nonPivotKeyValues = new ArrayList<Key>();
	//final List<Map<Key, Object>> valuesForEachMeasure = new ArrayList<Map<Key, Object>>();
	final Map<Key, Object> valuesByKey = new HashMap<Key, Object>();
	
	// pivot metadata properties
	final List<String> newColNames = new ArrayList<String>();
	final List<Integer> newColTypes = new ArrayList<Integer>();
	Key[] keysForDataColumns = null;
	final ResultSetMetaData metadata;
	
	// pivot ResultSet properties
	int position = -1;
	//int rowCount = 0;
	Key currentNonPivotKey = null;
	boolean showMeasuresInColumns = true;
	boolean showMeasuresFirst = true;
	boolean alwaysShowMeasures = false;
	//final boolean ignoreNullValues = true; // true: may use less memory...
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
		//XXX: add parameter: nullPlaceholder
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

			for(int j=0;j<colsNotToPivot.size();j++) {
				if(colsNotToPivot.get(j).equals(colName)) {
					int type = rsmd.getColumnType(i);
					colsNotToPivotType.set(j, type);
				}
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
				//valuesForEachMeasure.add(new HashMap<Key, Object>());
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
		
		//log.debug("rsColNames="+rsColNames);
		//log.debug("colsNotToPivot: names="+this.colsNotToPivot+" ; types="+colsNotToPivotType);
		//log.debug("colsToPivotNames: names="+this.colsToPivotNames);
		//log.debug("measureCols="+this.measureCols);
		
		if(doProcess) { process(); }
	}
	
	void setFlags(int flags) {
		//log.debug("flags: "+flags);
		showMeasuresInColumns = (flags & SHOW_MEASURES_IN_ROWS) == 0;
		showMeasuresFirst = (flags & SHOW_MEASURES_LAST) == 0;
		alwaysShowMeasures = (flags & SHOW_MEASURES_ALLWAYS) != 0;
		noColsWithNullValues = (flags & FLAG_NON_EMPTY_COLS) != 0;
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
			Object[] keyArr = processKeyArray(rs, 1);
			for(int i=0;i<measureCols.size();i++) {
				String measureCol = measureCols.get(i);
				Object value = rs.getObject(measureCol);
				//if(ignoreNullValues && value==null) { continue; }
				if(noColsWithNullValues && value==null) { continue; }
				
				//log.info("process: "+key+" add[0]: "+measureCol);
				Object[] karr = new Object[keyArr.length];
				System.arraycopy(keyArr, 0, karr, 0, keyArr.length);
				
				karr[0] = measureCol;
				Key key = new Key(karr);

				//Map<Key, Object> values = valuesForEachMeasure.get(i);
				Object prevValue = valuesByKey.get(key);
				//log.info("process: "+key+" ; value: "+value+" ; prevValue: "+prevValue);
				if(prevValue==null) {
					valuesByKey.put(key, value);
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
						valuesByKey.put(key, value);
						break;
					default:
						throw new IllegalStateException("unknown aggregator: "+aggregator);
					} 
				}
				
				//log.debug("put: key="+key+" ; val="+value+"; aggr="+aggregator);
			}
			
			if(showMeasuresInColumns) {
				if(colsNotToPivot.size()>0 || measureCols.size()>0) {
					Object[] knpt = new Object[colsNotToPivot.size()];
					System.arraycopy(keyArr, 1, knpt, 0, colsNotToPivot.size());
					Key keyNotToPivotArr = new Key(knpt);
					//log.info("new row? "+keyNotToPivotArr+" / "+colsNotToPivotKeySet);
					if(!colsNotToPivotKeySet.contains(keyNotToPivotArr)) {
						//new row!
						//rowCount++;
						//log.debug("[incol] nonPivotKeyValues.add: "+keyNotToPivotArr+" / keyArr = "+Arrays.asList(keyArr)+" / values = "+Arrays.asList(keyNotToPivotArr.values));
						nonPivotKeyValues.add(keyNotToPivotArr);
						colsNotToPivotKeySet.add(keyNotToPivotArr);
					}
				}
			}
			else {
				Object[] keyVals = new Object[colsNotToPivot.size()+1];
				System.arraycopy(keyArr, 1, keyVals, 0+(showMeasuresFirst?1:0), colsNotToPivot.size());
				if(showMeasuresFirst) {
					keyVals[0] = measureCols.size()>0?measureCols.get(0):null; //just put first measure (for now)
				}
				else {
					keyVals[keyVals.length-1] = measureCols.get((getRowCount()+1)%measureCols.size()); 
				}
				Key keyNotToPicotArr = new Key(keyVals);
				if(!colsNotToPivotKeySet.contains(keyNotToPicotArr)) {
					//new row!
					//rowCount++;
					//log.debug("[inrow] nonPivotKeyValues.add: "+keyNotToPicotArr);
					nonPivotKeyValues.add(keyNotToPicotArr);
					colsNotToPivotKeySet.add(keyNotToPicotArr);
				}
			}
			
			//log.debug("key: "+key);
			
			count++;
			
			//XXX: observer: count%COUNT_SIZE==0 ...
			
			if(count == 1) {
				log.debug("1st row processed");
			}
			if(count%logEachXRows == 0) {
				log.debug("processed row count: "+count);
			}
		}
		log.debug("all rows processed [count: "+count+"]");
		
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
						//log.debug("[!showMeasuresInColumns && showMeasuresFirst] nonPivotKeyValues.add: "+Arrays.asList(values));
						nonPivotKeyValues.add(new Key(values));
						//rowCount++;
					}
				}
			}
			else {
				//FIXedME populate nonPivotKeyValues when showMeasuresFirst is false. really?
				/*
				List<Key> newNonPivotKeyValues = new ArrayList<Key>();
				int origKeysLen = nonPivotKeyValues.size();
				//log.debug("[!showMeasuresInColumns && !showMeasuresFirst] nonPivotKeyValues.removeAll()");
				for(int i=0;i<origKeysLen;i++) {
					Key key = nonPivotKeyValues.get(i);
					for(int j=0;j<measureCols.size();j++) {
						Object[] values = Arrays.copyOf(key.values, key.values.length);
						values[values.length-1] = measureCols.get(j);
						newNonPivotKeyValues.add(new Key(values));
						//log.debug("[!showMeasuresInColumns && !showMeasuresFirst] nonPivotKeyValues.add: "+Arrays.asList(values)+" key: "+key);
						//rowCount++;
					}
				}
				nonPivotKeyValues.clear();
				nonPivotKeyValues.addAll(newNonPivotKeyValues);
				*/
				//rowCount = newNonPivotKeyValues.size();
			}
			//log.info("2 [showMeasuresInColumns="+showMeasuresInColumns+";showMeasuresFirst="+showMeasuresFirst+"] nonPivotKeyValues[#"+nonPivotKeyValues.size()+"]="+nonPivotKeyValues);
		}

		//log.info("nonPivotKeyValues (before): "+nonPivotKeyValues+" / "+Arrays.asList(nonPivotKeyValues.get(0).values[0])+" / "+nonPivotKeyValues.get(0).values[0].getClass());
		//log.debug("#nonPivotKeyValues = "+nonPivotKeyValues.size()+" (before sort)");
		if(sortNonPivotKeyValues) {
			//nonPivotKeyValues.sort(null); //java 8 ; java8
			Collections.sort(nonPivotKeyValues);
		}
		log.debug("#nonPivotKeyValues = "+nonPivotKeyValues.size());
		//log.debug("nonPivotKeyValues: "+nonPivotKeyValues);
		//log.debug("valuesByKey: "+valuesByKey);
		
		originalRSRowCount = count;
		processed = true;
		
		//log.info(">> valuesByKeys.keys: "+valuesByKey.keySet());
		//log.info(">> valuesByKeys: "+valuesByKey);
		//log.info(">> nonPivotKeyValues: "+nonPivotKeyValues);
		
		processMetadata();
	}
	
	public void processMetadata() {
		//-- create rs-metadata --
		newColNames.clear();
		newColTypes.clear();
		
		log.debug("processMetadata: #measures = "+measureCols.size());
		//log.debug("processMetadata: measures="+measureCols);
		
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
		
		//System.out.println( "keyColValues:\n" + keyColValues );
		//System.out.println( "valuesByKey:\n" + valuesByKey);
		//System.out.println( "noColsWithNullValues:\n" + noColsWithNullValues);
		
		List<String> dataColumns = null;
		if(noColsWithNullValues) {
			dataColumns = generateNewCols();
		}
		else {
			dataColumns = new ArrayList<String>();
			//foreach pivoted column
			genNewCols(0, "", dataColumns);
		}
		//System.out.println("dataColumns[#"+dataColumns.size()+"] = "+dataColumns);
		log.debug("#dataColumns = "+dataColumns.size());
		
		//single-measure
		if(measureCols.size()==1 || !showMeasuresInColumns) {
			log.debug("single-measure: "+measureCols+" ; showMeasuresInColumns="+showMeasuresInColumns);
			newColNames.addAll(dataColumns);
			if(measureCols.size()>=1) {
				for(int i=0;i<dataColumns.size();i++) {
					newColTypes.add(measureColsType.get(0));
				}
				if(dataColumns.size()==0) {
					newColNames.add(measureCols.get(0));
					newColTypes.add(measureColsType.get(0));
				}
			}
			else {
				for(int i=0;i<dataColumns.size();i++) {
					newColTypes.add(Types.VARCHAR);
				}
				if(dataColumns.size()==0) {
					newColNames.add(null);
					newColTypes.add(Types.VARCHAR);
				}
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
		//multi-measure (or zero-measure)
		else {
			log.debug("multi-measure: "+measureCols+" ; showMeasuresInColumns="+showMeasuresInColumns+" ; showMeasuresFirst="+showMeasuresFirst);
			//TODOne: multi-measure
			//List<String> colNames = new ArrayList<String>();
			//colNames.addAll(newColNames);
			//log.info("processMetadata:: measureCols="+measureCols+" ; dataColumns="+dataColumns+" ; newColNames="+newColNames);
			
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
				if(measureCols.size()==0) {
					for(int i=0;i<dataColumns.size();i++) {
						newColNames.add(dataColumns.get(i));
						newColTypes.add(Types.VARCHAR); // probably doesn't matter
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
				if(measureCols.size()==0) {
					for(int i=0;i<dataColumns.size();i++) {
						newColNames.add(dataColumns.get(i));
						newColTypes.add(Types.VARCHAR); // probably doesn't matter
					}
				}
			}

			//log.info("processMetadata[2]:: measureCols="+measureCols+" ; dataColumns="+dataColumns+" ; newColNames="+newColNames);
		}
		log.debug("#newColNames: "+newColNames.size()+" [noColsWithNullValues="+noColsWithNullValues+"]");
		
		/*if(noColsWithNullValues) {
			removeColumnsWithNoValues();
		}*/
		
		int nonDataColumns = colsNotToPivot.size()+(showMeasuresInColumns?0:1);
		keysForDataColumns = new Key[newColNames.size()];
		//log.info("nonDataColumns="+nonDataColumns+" ; newColNames.size()="+newColNames.size());
		log.debug("nonDataColumns="+nonDataColumns+" ; newColNames.size()="+newColNames.size());
		
		//System.out.println("newColNames: "+newColNames);
		for(int i=nonDataColumns;i<newColNames.size();i++) {
			String colName = newColNames.get(i);
			keysForDataColumns[i] = getKeyFromDataColumnName(colName);
			//log.info("["+i+"/"+nonDataColumns+"] colName="+colName+" ; idx="+(i-nonDataColumns)+" // "+keysForDataColumns[i]);
		}
		log.debug("#keysForDataColumns="+keysForDataColumns.length);
		
		resetPosition();

		log.debug("processMetadata: #columns(newColNames)="+newColNames.size()+" #types="+newColTypes.size());

		//log.debug("keysForDataColumns: "+Arrays.asList(keysForDataColumns));
		//log.debug("processMetadata: columns(newColNames)="+newColNames+" types="+newColTypes);
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
		if(colVals!=null) {
		//log.debug("colVals="+colVals);
		for(Object v: colVals) {
			String colFullName = partialColName+(colNumber==0?"":COLS_SEP)+colName+COLVAL_SEP+valueToString(v);
			if(colNumber+1==colsToPivotCount) {
				//add col name
				//XXX test for noColsWithNullValues?
				newColumns.add(colFullName);
				/*if(!noColsWithNullValues || columnContainsValues(colFullName, colsNotToPivot.size()+colNumber)) {
					newColumns.add(colFullName);
					//log.debug("genNewCols: colFullName = "+colFullName);
				}
				else {
					log.debug("genNewCols: no values! [col-full-name: "+colFullName+"]");
				}*/
			}
			else {
				//log.info("col-partial-name: "+colFullName);
				genNewCols(colNumber+1, colFullName, newColumns);
			}
		}
		}
	}
	
	List<String> generateNewCols() {
		List<String> newColumns = new ArrayList<>();
		if(colsToPivotNames.size()==0) {
			return newColumns;
		}
		int measurel = 1;
		int cntpl = colsNotToPivot.size();
		int offset = measurel+cntpl;
		int ctpl = colsToPivotNames.size();
		//int colsz = measurel+cntpl+ctpl;
		//Set<Key> keys = valuesByKey.keySet();
		List<Key> keys = new ArrayList<>();
		for(Key key: valuesByKey.keySet()) {
			keys.add(key.copy());
		}
		//log.debug("keys[0]:: "+keys);
		for(Key key: keys) {
			for(int i=0;i<measurel+cntpl;i++) {
				key.values[i] = null;
			}
		}
		//log.debug("keys[1]:: "+keys);
		Collections.sort(keys);
		//log.debug("keys[2]:: "+keys);
		//for(Map.Entry<Key, Object> e: valuesByKey.entrySet()) {
		for(Key key: keys) {
			//Key key = e.getKey();
			//Object val = e.getValue(); // check if value null? check ignoreNullValues??
			StringBuilder sb = new StringBuilder();
			//Object[] values = new Object[key.values.length];
			//System.arraycopy(key.values, 0, values, 0, key.values.length);
			//log.debug(Arrays.asList(values));
			//Arrays.sort(values, nullOkComparator);
			sb.append( colsToPivotNames.get(0) + COLVAL_SEP + valueToString(key.values[offset]));
			for(int i=1;i<ctpl;i++) {
				sb.append( COLS_SEP + colsToPivotNames.get(i) + COLVAL_SEP + valueToString(key.values[offset+i]));
			}
			String nc = sb.toString();
			if(!newColumns.contains(nc)) {
				newColumns.add(sb.toString());
			}
			//(colNumber==0?"":COLS_SEP)+colName+COLVAL_SEP+valueToString(v)
		}
		return newColumns;
	}
	
	String valueToString(Object o) {
		return o!=null ? String.valueOf(o) : NULL_PLACEHOLDER;
	}
	
	/*boolean columnContainsValues(String fullCollName, int pivotColIdx) {
		log.debug("fullCollName: "+fullCollName+" ; valuesByKey: "+valuesByKey);
		for(int i=0;i<measureCols.size();i++) {
			//String measureCol = measureCols.get(i);
			Map<Key, Object> values = valuesForEachMeasure.get(i);
			//Map<Key, Object> values = valuesByKey.entrySet();
			for(Map.Entry<Key, Object> e: values.entrySet()) {
				// key values order: colsNotToPivot, colsToPivotNames
				Object v = e.getKey().values[pivotColIdx];
				if(v!=null) {
					log.debug("fullCollName: "+fullCollName+"/"+pivotColIdx+" ; e.getKey(): "+e.getKey());
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
		log.debug("removeColumnsWithNoValues: #colsNotToPivot: "+cntpl+" ; #colsToPivotNames: "+colsToPivotNames.size()+" ; #newColNames: "+newColNames.size());
		for(int i=newColNames.size()-1;i>=cntpl+(showMeasuresInColumns?0:1);i--) {
			String col = newColNames.get(i);
			String[] parts = col.split(COLS_SEP_PATTERN);
			String[] vals = new String[colsToPivotNames.size()];
			//String measure = null;
			int vidx = 0;
			for(int j=0;j<parts.length;j++) {
				String[] cv = parts[j].split(COLVAL_SEP_PATTERN);
				if(cv.length==2) {
					String val = cv[1];
					if(NULL_PLACEHOLDER.equals(val)) { val = null; }
					vals[vidx++] = val;
				}
				/*else if(cv.length==1) {
					measure = cv[0];
				}*/
			}
			//log.debug("col: "+col+" ; vals: "+Arrays.asList(vals));
			
			boolean match = false;
			//measurecols:
			//for(int j=0;j<measureCols.size();j++) {
				//String measureCol = measureCols.get(i);
				//Map<Key, Object> values = valuesForEachMeasure.get(j);
				for(Map.Entry<Key, Object> e: valuesByKey.entrySet()) {
					//log.debug("e.getKey(): "+e.getKey()+" ; cntpl: "+cntpl);
					//if( arrayPartialEquals(vals, 0, e.getKey().values, cntpl) ) {
					if( isArrayTail(toStringArray(e.getKey().values), vals) ) {
						match = true;
						//log.info("match: "+Arrays.asList(vals)+" // "+Arrays.asList(e.getKey().values));
						break;
					}
				}
			//}
			if(!match) {
				newColNames.remove(i);
				newColTypes.remove(i);
			}
		}
		log.debug("after removeColumnsWithNoValues: #colsNotToPivot: "+cntpl+" ; #colsToPivotNames: "+colsToPivotNames.size()+" ; #newColNames: "+newColNames.size());
		
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
			if(arr[i]!=null) {
				ret[i] = arr[i].toString();
			}
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
	
	Object[] processKeyArray(ResultSet rs, int prepend) throws SQLException {
		int cntpl = colsNotToPivot.size();
		int ctpl = colsToPivotNames.size();
		
		Object[] ret = new Object[cntpl+ctpl+prepend];
		//log.info("not to pivot: "+colsNotToPivot);
		for(int i=0;i<cntpl;i++) {
			String col = colsNotToPivot.get(i);
			Object val = rs.getObject(col);
			validateKeyValue(col, val);
			addValueToKeyColSet(col, val);
			ret[i+prepend] = val;
		}
		int toAdd = cntpl+prepend;
		//log.info("to pivot: "+colsToPivotNames);
		for(int i=0;i<ctpl;i++) {
			String col = colsToPivotNames.get(i);
			Object val = rs.getObject(col);
			validateKeyValue(col, val);
			addValueToKeyColSet(col, val);
			ret[toAdd+i] = val;
		}
		return ret;
	}
	
	void validateKeyValue(String col, Object val) {
		if(val==null) {
			//log.debug("value for key col is null [col = "+col+"]");
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
	
	void addValueToKeyColSet(String col, Object val) {
		//adds value to set
		Set<Object> vals = keyColValues.get(col);
		if(vals==null) {
			//XXX: order of elements inside set: use Comparator/Comparable
			//vals = new HashSet<Object>();
			//vals = new TreeSet<Object>();
			vals = new TreeSet<Object>(nullOkComparator);
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
			if(o!=null && value.equals(o.toString())) {
				return o;
			}
		}
		return null;
	}
	
	Key getKeyFromDataColumnName(String columnLabel) {
		Key key = null;
		
		int measureIndex = -1; 
		List<Object> pkVals = new ArrayList<Object>();
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
				if(colAndVal.length==2) {
					Object value = findPivotKeyValueFromStringValue(colAndVal[0], colAndVal[1]);
					pkVals.add(value);
				}
				else {
					log.warn("can't split [col="+columnLabel+"]: "+p);
				}
			}
		}
		//log.debug("getObject: sb: "+sb);
		int cntpl = colsNotToPivot.size() + (showMeasuresInColumns?0:1);
		Object[] keyArr = new Object[cntpl+pkVals.size()+1];
		if(currentNonPivotKey!=null) {
			if(cntpl!=currentNonPivotKey.values.length) {
				log.warn("cntpl: "+cntpl+" ; "+currentNonPivotKey.values.length);
			}
			System.arraycopy(currentNonPivotKey, 0, keyArr, 1, cntpl);
			/*for(int i=0;i<cntpl;i++) {
				keyArr[i+1] = currentNonPivotKey.values[i];
			}*/
		}
		// System.arraycopy(pkVals.toArray(), 0, keyArr, cntpl+1, pkVals.size()); //XXX?
		for(int i=0;i<pkVals.size();i++) {
			keyArr[i+cntpl+1] = pkVals.get(i);
		}
		if(!showMeasuresInColumns) {
			//remove measure name from key
			if(showMeasuresFirst) {
				keyArr = Arrays.copyOfRange(keyArr, 1, keyArr.length);
			}
			else {
				int npkc = cntpl-1;
				Object[] newKeyArr = new Object[keyArr.length-1];
				Object measure = keyArr[npkc+1];
				System.arraycopy(keyArr, 1, newKeyArr, 1, npkc);
				//log.debug("[int;"+npkc+"] before: "+Arrays.asList(keyArr)+" ; after: "+Arrays.asList(newKeyArr));
				System.arraycopy(keyArr, npkc+2, newKeyArr, npkc+1, keyArr.length-npkc-2);
				newKeyArr[0] = measure;
				//log.debug("getObject: measureKey: before: "+Arrays.asList(keyArr)+" ; after: "+Arrays.asList(newKeyArr));
				keyArr = newKeyArr;
			}
		}
		else {
			if(measureIndex==-1) {
				//measure name in its own column
				//if((!showMeasuresInColumns)) {
				//	if(showMeasuresFirst) {
				//		measureIndex = position/(nonPivotKeyValues.size()/measureCols.size());
				//	}
				//	else {
				//		measureIndex = position%measureCols.size();
				//	}
				//}
				//single measure
				//else {
					measureIndex = 0;
				//}
			}
			//log.debug("pivotCol0: keyArr:: "+Arrays.asList(keyArr)+" measureIndex="+measureIndex);
			if(measureCols.size()>measureIndex) {
				keyArr[0] = measureCols.get(measureIndex);
			}
			else {
				//log.debug("no measures? measureCols="+measureCols);
			}
		}
		key = new Key(keyArr);
		return key;
	}
	
	Key mergeKeyWithCurrentNonPivotKey(Key key) {
		Key ret = new Key(Arrays.copyOf(key.values, key.values.length));
		
		int offset = (showMeasuresInColumns?1:0);
		int len = currentNonPivotKey.values.length;
		if(!showMeasuresInColumns && !showMeasuresFirst) {
			ret.values[0] = currentNonPivotKey.values[currentNonPivotKey.values.length-1];
			offset = 1;
			len--;
		}
		System.arraycopy(currentNonPivotKey.values, 0, ret.values, offset, len);
		/*for(int i=0;i<len;i++) {
			ret.values[i+offset] = currentNonPivotKey.values[i];
		}*/
		return ret;
	}
	
	@Override
	public Object getObject(String columnLabel) throws SQLException {
		//log.info("getObject: "+columnLabel);
		int index = colsNotToPivot.indexOf(columnLabel);
		if(index>=0) {
			//is nonpivotcol
			
			//log.debug("getObject[non-pivot]: "+columnLabel+" [nonPivotKey:"+index+"] :"+colsNotToPivot+":cnpk="+currentNonPivotKey+":"+java.util.Arrays.asList(currentNonPivotKey.split("\\|")));
			//XXXdone: return non-string values for non-pivot columns?
			if(!showMeasuresInColumns && showMeasuresFirst) {
				//log.debug("getObject[non-pivot1]: "+columnLabel+" [nonPivotKey:"+index+"] : "+Arrays.asList( currentNonPivotKey.values )+ " ::: "+currentNonPivotKey.values[index+1]);
				return currentNonPivotKey.values[index+1];
			}
			//log.debug("getObject[non-pivot]: "+columnLabel+" [nonPivotKey:"+index+"] : "+currentNonPivotKey.values[index]+" / "+Arrays.asList( currentNonPivotKey.values )+ " ::: "+currentNonPivotKey.values[index]);
			return currentNonPivotKey.values[index];
		}
		
		// measures column?
		if((!showMeasuresInColumns) && MEASURES_COLNAME.equals(columnLabel)) {
			if(showMeasuresFirst) {
				if(measureCols.size()==0) { return null; }
				return measureCols.get(position/(nonPivotKeyValues.size()/measureCols.size()) );
			}
			else {
				return measureCols.get(position%measureCols.size());
			}
		}
		
		index = newColNames.indexOf(columnLabel);
		//Key key0 = null;
		Key key1 = null;
		if(index>=0) {
			// strategy 0: get key from values inside column name
			//key0 = getKeyFromDataColumnName(columnLabel);
			// strategy 1: get key from key cache (keysForDataColumns)
			key1 = keysForDataColumns[index];
			if(key1!=null) {
				key1 = mergeKeyWithCurrentNonPivotKey(key1);
				/*key1 = new Key(Arrays.copyOf(key1.values, key1.values.length));
				int offset = (showMeasuresInColumns?1:0);
				int len = currentNonPivotKey.values.length;
				if(!showMeasuresInColumns && !showMeasuresFirst) {
					key1.values[0] = currentNonPivotKey.values[currentNonPivotKey.values.length-1];
					offset = 1;
					len--;
				}
				for(int i=0;i<len;i++) {
					key1.values[i+offset] = currentNonPivotKey.values[i];
				}*/
			}
		}
		/*if(key0!=null && key1!=null) {
			if(!key0.equals(key1)) {
				log.warn("keys differ! key0="+key0+" ;; key1="+key1+" ;; idx="+index+" /// "+keysForDataColumns[index]+" /// "+Arrays.asList(currentNonPivotKey.values));
			}
		}
		else {
			if(key0!=null || key1!=null) {
				log.warn("keys differ[2]! key0="+key0+" ;; key1="+key1+" ;; idx="+index);
			}
		}*/
		
		/*index = newColNames.indexOf(columnLabel);
		if(index>=0) {
			//is pivotcol
			
			int measureIndex = -1; 
			List<Object> pkVals = new ArrayList<Object>();
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
					pkVals.add(value);
				}
			}
			//log.debug("getObject: sb: "+sb);
			Object[] keyArr = new Object[currentNonPivotKey.values.length+pkVals.size()+1];
			for(int i=0;i<currentNonPivotKey.values.length;i++) {
				keyArr[i+1] = currentNonPivotKey.values[i];
			}
			for(int i=0;i<pkVals.size();i++) {
				keyArr[i+currentNonPivotKey.values.length+1] = pkVals.get(i);
			}
			if(!showMeasuresInColumns) {
				//remove measure name from key
				if(showMeasuresFirst) {
					keyArr = Arrays.copyOfRange(keyArr, 1, keyArr.length);
				}
				else {
					int npkc = currentNonPivotKey.values.length-1;
					Object[] newKeyArr = new Object[keyArr.length-1];
					Object measure = keyArr[npkc+1];
					System.arraycopy(keyArr, 1, newKeyArr, 1, npkc);
					//log.debug("[int;"+npkc+"] before: "+Arrays.asList(keyArr)+" ; after: "+Arrays.asList(newKeyArr));
					System.arraycopy(keyArr, npkc+2, newKeyArr, npkc+1, keyArr.length-npkc-2);
					newKeyArr[0] = measure;
					//log.debug("getObject: measureKey: before: "+Arrays.asList(keyArr)+" ; after: "+Arrays.asList(newKeyArr));
					keyArr = newKeyArr;
				}
			}
			else {
				if(measureIndex==-1) {
					//measure name in its own column
					//if((!showMeasuresInColumns)) {
					//	if(showMeasuresFirst) {
					//		measureIndex = position/(nonPivotKeyValues.size()/measureCols.size());
					//	}
					//	else {
					//		measureIndex = position%measureCols.size();
					//	}
					//}
					//single measure
					//else {
						measureIndex = 0;
					//}
				}
				//log.debug("pivotCol0: keyArr:: "+Arrays.asList(keyArr));
				keyArr[0] = measureCols.get(measureIndex);
			}
			Key key = new Key(keyArr);
			Object value = valuesByKey.get(key);
			//log.debug("pivotCol: key:: "+key+" / value: "+value+" / currentNonPivotKey: "+currentNonPivotKey);

			return value;
			
			//Map<Key, Object> measureMap = valuesForEachMeasure.get(measureIndex);
			//log.debug("getObject[pivot] value [key="+key+",measureIndex="+measureIndex+"]: "+measureMap.get(key));
			//return measureMap.get(key);
		}*/
		if(key1!=null) {
			Object value = valuesByKey.get(key1);
			return value;
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
