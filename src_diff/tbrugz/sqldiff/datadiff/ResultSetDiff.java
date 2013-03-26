package tbrugz.sqldiff.datadiff;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import tbrugz.sqldump.SQLUtils;

public class ResultSetDiff {

	static final Log log = LogFactory.getLog(ResultSetDiff.class);
	
	//XXX: add option to set limit
	long limit = 200;
	
	public void diff(ResultSet source, ResultSet target, List<String> keyCols) throws SQLException {
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
		
		long count = 0;
		while(true) {
			if(readSource) {
				hasNextSource = source.next();
				sourceVals = cast(SQLUtils.getRowObjectListFromRS(source, lsColTypes, numCol));
			}
			if(readTarget) {
				hasNextTarget = target.next();
				targetVals = cast(SQLUtils.getRowObjectListFromRS(target, lsColTypes, numCol));
			}
			
			if(!hasNextSource && !hasNextTarget) { break; }
			
			int compare = compareVals(sourceVals, targetVals, keyIndexes);
			if(compare==0) {
				//same key
				readSource = readTarget = true;
				//XXX compare other cols; if diff, dump update
				log.info("update? "+sourceVals+" / "+targetVals+" // "+hasNextSource+"/"+hasNextTarget);
			}
			else if(compare<0) {
				readSource = true; readTarget = false;
				//XXX dump insert/delete
				if(hasNextSource) {
					log.info("delete: ->"+sourceVals+" / "+targetVals+" // "+hasNextSource+"/"+hasNextTarget);
				}
				else {
					readSource = false; readTarget = true;
					log.info("insert: "+sourceVals+" / ->"+targetVals+" // "+hasNextSource+"/"+hasNextTarget);
				}
			}
			else {
				readSource = false; readTarget = true;
				//XXX dump insert/delete
				if(hasNextTarget) {
					log.info("insert: "+sourceVals+" / ->"+targetVals+" // "+hasNextSource+"/"+hasNextTarget);
				}
				else {
					readSource = true; readTarget = false;
					log.info("delete: ->"+sourceVals+" / "+targetVals+" // "+hasNextSource+"/"+hasNextTarget);
				}
				
				if(!hasNextTarget) { readSource = true; readTarget = false; }
			}
			
			count++;
			if(limit>0 && count>limit) {
				log.info("limit reached: "+limit);
				break;
			}
		}
		
	}

	//TODO: implement getXXX methods

	public int getIdenticalRowsCount() {
		return 0;
	}

	public int getUpdateCount() {
		return 0;
	}
	
	public int getInsertCount() {
		return 0;
	}

	public int getDeleteCount() {
		return 0;
	}

	public int getSourceRowCount() {
		return 0;
	}

	public int getTargetRowCount() {
		return 0;
	}
	
	/*static String getKeyValue(ResultSet rs, List<String> keyCols) {
		List<String> keyvalue = new ArrayList<String>();
		
		for(String col: keyCols) {
			keyvalue.add(rs.getS)
		}
	}*/
	
	int compareVals(List<Comparable> vals1, List<Comparable> vals2, int[] keyIndexes) {
		int comp = 0;
		for(int i=0;i<keyIndexes.length; i++) {
			comp = vals1.get(keyIndexes[i]).compareTo(vals2.get(keyIndexes[i]));
			if(comp!=0) return comp;
		}
		return comp;
	}
	
	static List<Comparable> cast(List<Object> list) {
		List<Comparable> ret = new ArrayList<Comparable>();
		for(Object o: list) {
			ret.add((Comparable)o);
		}
		return ret;
	}
}
