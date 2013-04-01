package tbrugz.sqldiff.datadiff;

import java.io.IOException;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import tbrugz.sqldump.util.CategorizedOut;
import tbrugz.sqldump.util.SQLUtils;

public class ResultSetDiff {

	static final Log log = LogFactory.getLog(ResultSetDiff.class);
	
	//XXX: option to set 'logEachXLoops' 
	int logEachXLoops = 1000;
	
	long limit = 0;
	int identicalRowsCount, updateCount, dumpCount, deleteCount, sourceRowCount, targetRowCount;
	
	//XXX: add schemaName ? properties or DiffSyntax ?
	@SuppressWarnings("rawtypes")
	public void diff(ResultSet source, ResultSet target, String tableName, List<String> keyCols,
			CategorizedOut cout) throws SQLException, IOException {
		boolean readSource = true;
		boolean readTarget = true;
		boolean hasNextSource = true;
		boolean hasNextTarget = true;
		List<Comparable> sourceVals = null;
		List<Comparable> targetVals = null;
		
		ResultSetMetaData md = source.getMetaData();
		int numCol = md.getColumnCount();
		List<String> lsColNames = new ArrayList<String>();
		List<Class<?>> lsColTypes = new ArrayList<Class<?>>();
		for(int i=0;i<numCol;i++) {
			lsColNames.add(md.getColumnName(i+1));
		}
		for(int i=0;i<numCol;i++) {
			lsColTypes.add(SQLUtils.getClassFromSqlType(md.getColumnType(i+1), md.getPrecision(i+1), md.getScale(i+1)));
		}
		
		int[] keyIndexes = new int[keyCols.size()];
		for(int i=0;i<keyCols.size();i++) {
			String key = keyCols.get(i);
			keyIndexes[i] = lsColNames.indexOf(key);
		}
		
		DiffSyntax ds = getSyntax(new Properties(), tableName, keyCols, md); //XXX: properties?
		//Writer w = new PrintWriter(System.out); //XXXxx: change to COut ? - [schemaname](?), [tablename], [changetype]
		identicalRowsCount = updateCount = dumpCount = deleteCount = sourceRowCount = targetRowCount = 0;
		long count = 0;
		
		while(true) {
			if(readSource) {
				hasNextSource = source.next();
				if(hasNextSource) {
					sourceRowCount++;
					sourceVals = cast(SQLUtils.getRowObjectListFromRS(source, lsColTypes, numCol));
				}
			}
			if(readTarget) {
				hasNextTarget = target.next();
				if(hasNextTarget) {
					targetRowCount++;
					targetVals = cast(SQLUtils.getRowObjectListFromRS(target, lsColTypes, numCol));
				}
			}
			
			if(!hasNextSource && !hasNextTarget) { break; }
			
			int compare = 0;
			try {
				compare = compareVals(sourceVals, targetVals, keyIndexes);
			}
			catch(ClassCastException e) {
				log.error("error compating rows: source="+sourceVals+" ; target="+targetVals+" ; [ex="+e+"]");
				return;
			}
			
			if(compare==0) {
				//same key
				readSource = readTarget = true;
				boolean updated = ds.dumpUpdateRowIfNotEquals(source, target, count, cout.getCategorizedWriter("", tableName, "update"));
				log.debug("update? "+sourceVals+" / "+targetVals+(updated?" [updated]":"")+" // "+hasNextSource+"/"+hasNextTarget);
				if(updated) { updateCount++; }
				else { identicalRowsCount++; }
			}
			else if(compare<0) {
				readSource = true; readTarget = false;
				if(hasNextSource) {
					log.debug("delete: ->"+sourceVals+" / "+targetVals+" // "+hasNextSource+"/"+hasNextTarget);
					ds.dumpDeleteRow(source, count, cout.getCategorizedWriter("", tableName, "delete"));
					deleteCount++;
				}
				else {
					readSource = false; readTarget = true;
					log.debug("insert: "+sourceVals+" / ->"+targetVals+" // "+hasNextSource+"/"+hasNextTarget);
					ds.dumpRow(target, count, cout.getCategorizedWriter("", tableName, "insert"));
					dumpCount++;
				}
			}
			else {
				readSource = false; readTarget = true;
				if(hasNextTarget) {
					log.debug("insert: "+sourceVals+" / ->"+targetVals+" // "+hasNextSource+"/"+hasNextTarget);
					ds.dumpRow(target, count, cout.getCategorizedWriter("", tableName, "insert"));
					dumpCount++;
				}
				else {
					readSource = true; readTarget = false;
					log.debug("delete: ->"+sourceVals+" / "+targetVals+" // "+hasNextSource+"/"+hasNextTarget);
					ds.dumpDeleteRow(source, count, cout.getCategorizedWriter("", tableName, "delete"));
					deleteCount++;
				}
				
				if(!hasNextTarget) { readSource = true; readTarget = false; }
			}
			
			count++;
			
			if( (logEachXLoops>0) && (count>0) &&(count%logEachXLoops==0) ) { 
				log.info("[table="+tableName+"] "+count+" rows compared ; stats: "+getCompactStats());
			}
			
			if(limit>0 && count>limit) {
				log.info("limit reached: "+limit+" [table="+tableName+"]");
				break;
			}
		}
		//XXX ds.dumpFooter(w); ??? cout.getAllWriters() ?
		//w.flush();
	}
	
	public void setLimit(long limit) {
		this.limit = limit;
	}

	public int getIdenticalRowsCount() {
		return identicalRowsCount;
	}

	public int getUpdateCount() {
		return updateCount;
	}
	
	public int getDumpCount() {
		return dumpCount;
	}

	public int getDeleteCount() {
		return deleteCount;
	}

	public int getSourceRowCount() {
		return sourceRowCount;
	}

	public int getTargetRowCount() {
		return targetRowCount;
	}
	
	public String getStats() {
		return "dumps="+dumpCount+"; updates="+updateCount+"; deletes="+deleteCount
				+"; identicalRows="+identicalRowsCount
				+" [sourceCount="+sourceRowCount+"; targetCount="+targetRowCount+"]";
	}

	String getCompactStats() {
		return "IUDE/ST="+dumpCount+","+updateCount+","+deleteCount+","+identicalRowsCount
				+" / "+sourceRowCount+","+targetRowCount;
	}
	
	/*static String getKeyValue(ResultSet rs, List<String> keyCols) {
		List<String> keyvalue = new ArrayList<String>();
		
		for(String col: keyCols) {
			keyvalue.add(rs.getS)
		}
	}*/
	
	@SuppressWarnings({ "rawtypes", "unchecked" })
	static int compareVals(List<Comparable> vals1, List<Comparable> vals2, int[] keyIndexes) {
		int comp = 0;
		for(int i=0;i<keyIndexes.length; i++) {
			comp = vals1.get(keyIndexes[i]).compareTo(vals2.get(keyIndexes[i]));
			if(comp!=0) return comp;
		}
		return comp;
	}
	
	@SuppressWarnings("rawtypes")
	static List<Comparable> cast(List<Object> list) {
		List<Comparable> ret = new ArrayList<Comparable>();
		for(Object o: list) {
			ret.add((Comparable)o);
		}
		return ret;
	}
	
	static DiffSyntax getSyntax(Properties prop, String tableName, List<String> pkCols, ResultSetMetaData md) throws SQLException {
		DiffSyntax ds = new SQLDataDiffSyntax();
		ds.procProperties(prop);
		ds.initDump(tableName, pkCols, md);
		return ds;
	}
}
