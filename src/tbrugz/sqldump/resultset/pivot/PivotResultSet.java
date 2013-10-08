package tbrugz.sqldump.resultset.pivot;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
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
import tbrugz.sqldump.resultset.RSMetaDataTypedAdapter;

/*
 * ~TODO: after PivotRS is built, add possibility for change key-col <-> key-row (pivotting) - processMetadata()!
 * ~TODO: multiple column type (beside String): Integer, Double, Date
 * TODO: allow null key (pivotted) values (add StringComparatorNullFirst/Last ?)
 * XXX: option to remove cols/rows/both where all measures are null
 * ~XXX: aggregate if duplicated key found? first(), last()?
 * TODO: option to show measures in columns
 */
public class PivotResultSet extends AbstractResultSet {
	
	final static Log log = LogFactory.getLog(PivotResultSet.class);
	
	public enum Aggregator {
		FIRST,
		LAST
		//COUNT?
		//numeric: AVG, MAX, MIN, SUM, ...
	}

	final static String COLS_SEP = "|";
	final static String COLS_SEP_PATTERN = Pattern.quote(COLS_SEP);
	final static String COLVAL_SEP = ":";
	final static String COLVAL_SEP_PATTERN = Pattern.quote(COLVAL_SEP);

	final static int logEachXRows = 1000;
	
	final ResultSet rs;
	final int rsColsCount;
	
	final List<String> colsNotToPivot;
	final List<Integer> colsNotToPivotType = new ArrayList<Integer>(); 
	final Map<String, Comparable> colsToPivot;
	final List<String> measureCols = new ArrayList<String>();
	final List<Integer> measureColsType = new ArrayList<Integer>();

	transient final List<String> colsToPivotNames;
	
	final Map<String, Set<Object>> keyColValues = new HashMap<String, Set<Object>>();
	final List<Map<String, Object>> valuesForEachMeasure = new ArrayList<Map<String, Object>>(); //new HashMap<String, String>();
	final List<String> newColNames = new ArrayList<String>();
	final List<Integer> newColTypes = new ArrayList<Integer>();
	final ResultSetMetaData metadata;
	
	boolean processed = false;
	int position = -1;
	int originalRSRowCount = 0;
	int rowCount = 0;
	final List<String> nonPivotKeyValues = new ArrayList<String>();
	//final List<List<String>> nonPivotValues = new ArrayList<List<String>>();
	String currentNonPivotKey = null;
	public boolean showMeasuresInColumns = true; //XXX: show measures in columns
	public boolean showMeasuresFirst = true;
	public boolean alwaysShowMeasures = false;
	public static Aggregator aggregator = Aggregator.LAST;

	//colsNotToPivot - key cols
	public PivotResultSet(ResultSet rs, List<String> colsNotToPivot, List<String> colsToPivot, boolean doProcess) throws SQLException {
		this(rs, colsNotToPivot, list2Map(colsToPivot), doProcess);
	}
	
	public PivotResultSet(ResultSet rs, List<String> colsNotToPivot, Map<String, Comparable> colsToPivot,
			boolean doProcess) throws SQLException {
		this.rs = rs;
		this.colsNotToPivot = colsNotToPivot;
		this.colsToPivot = colsToPivot;
		this.colsNotToPivotType.clear();
		for(int i=0;i<colsNotToPivot.size();i++) { this.colsNotToPivotType.add(null); }
		
		List<String> colsTP = new ArrayList<String>();
		for(String key: colsToPivot.keySet()) {
			colsTP.add(key);
		}
		colsToPivotNames = colsTP;
		
		ResultSetMetaData rsmd = rs.getMetaData();
		rsColsCount = rsmd.getColumnCount();
		List<String> colsToPivotNotFound = new ArrayList<String>();
		List<String> colsNotToPivotNotFound = new ArrayList<String>();
		colsToPivotNotFound.addAll(colsToPivotNames);
		colsNotToPivotNotFound.addAll(colsNotToPivot);
		for(int i=1;i<=rsColsCount;i++) {
			String colName = rsmd.getColumnName(i);

			int index = colsNotToPivot.indexOf(colName);
			if(index>=0) {
				//colsNotToPivotType.set(index, Types.VARCHAR); //XXXxx set non-pivot column type
				int type = rsmd.getColumnType(index+1);
				colsNotToPivotType.set(index, type);
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
				valuesForEachMeasure.add(new HashMap<String, Object>());
			}
			log.debug("orig colName ["+i+"/"+rsColsCount+"]: "+colName);
		}
		
		if(colsToPivotNotFound.size()>0) {
			throw new RuntimeException("cols to pivot not found: "+colsToPivotNotFound);
		}
		if(colsNotToPivotNotFound.size()>0) {
			throw new RuntimeException("cols not to pivot not found: "+colsNotToPivotNotFound);
		}
		
		metadata = new RSMetaDataTypedAdapter(null, null, newColNames, newColTypes);
		
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
				Map<String, Object> values = valuesForEachMeasure.get(i);
				Object value = rs.getObject(measureCol);
				
				Object prevValue = values.get(key);
				if(prevValue==null) {
					values.put(key, value);
				}
				else {
					log.warn("prevValue not null[measurecol="+measureCol+";key="+key+"]: "+prevValue);
					switch (aggregator) {
					case FIRST:
						//do nothing
						break;
					case LAST:
						values.put(key, value);
					default:
						break;
					} 
				}
				
				//log.debug("put: key="+key+" ; val="+value+"; aggr="+aggregator);
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
			
			//log.debug("key: "+key);
			
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
		//-- create rs-metadata --
		newColNames.clear();
		newColTypes.clear();
		
		log.debug("processMetadata: measures="+measureCols);
		
		//create non pivoted col names
		for(int i=0;i<colsNotToPivot.size();i++) {
			String col = colsNotToPivot.get(i);
			newColNames.add(col);
			//newColTypes.add(Types.VARCHAR); //XXX set non-pivot column type
			newColTypes.add(colsNotToPivotType.get(i));
		}
		
		List<String> dataColumns = new ArrayList<String>();
		//foreach pivoted column
		genNewCols(0, "", dataColumns);
		
		//single-measure
		if(measureCols.size()==1) {
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

		log.debug("processMetadata: columns="+newColNames+" types="+newColTypes);
	}
	
	void genNewCols(int colNumber, String partialColName, List<String> newColumns) {
		String colName = colsToPivotNames.get(colNumber);
		Set<Object> colVals = keyColValues.get(colName);
		for(Object v: colVals) {
			String colFullName = partialColName+(colNumber==0?"":COLS_SEP)+colName+COLVAL_SEP+v;
			if(colNumber+1==colsToPivotNames.size()) {
				//add col name
				log.debug("genNewCols: col-full-name: "+colFullName);
				newColumns.add(colFullName);
			}
			else {
				//log.debug("col-partial-name: "+colFullName);
				genNewCols(colNumber+1, colFullName, newColumns);
			}
		}
	}
	
	String getKey() throws SQLException {
		StringBuilder sb = new StringBuilder();
		for(int i=0;i<colsNotToPivot.size();i++) {
			String col = colsNotToPivot.get(i);
			Object val = rs.getObject(col);
			if(val==null) { log.warn("value for key col is null [col = "+col+"]"); }
			addValueToSet(col, val);
			sb.append( (i==0?"":"|") + val);
		}
		sb.append("%");
		for(int i=0;i<colsToPivotNames.size();i++) {
			String col = colsToPivotNames.get(i);
			Object val = rs.getObject(col);
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
	public Object getObject(String columnLabel) throws SQLException {
		int index = colsNotToPivot.indexOf(columnLabel);
		//log.debug("getObject:: "+columnLabel+"/"+index+" :"+colsNotToPivot+":cnpk="+currentNonPivotKey+":"+Arrays.asList(currentNonPivotKey.split("\\|")));
		if(index>=0) {
			//is nonpivotcol
			//XXX: return non-string values for non-pivot columns?
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
			//log.debug("pivotCol: key:: "+key);
			
			//single measure
			if(measureIndex==-1) { measureIndex = 0; }
			//log.info("value [key="+key+",measureIndex="+measureIndex+"]:"+valuesForEachMeasure.get(measureIndex).get(key));
			return valuesForEachMeasure.get(measureIndex).get(key);
			
			//XXXxx multi-measure ? done ?
		}
		throw new SQLException("unknown column: '"+columnLabel+"'");
	}

	@Override
	public Object getObject(int columnIndex) throws SQLException {
		//log.debug("index: "+columnIndex+" // "+colsNotToPivot+":"+newColNames);
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
	
}
