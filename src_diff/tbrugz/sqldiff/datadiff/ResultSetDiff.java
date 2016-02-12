package tbrugz.sqldiff.datadiff;

import java.io.IOException;
import java.io.Writer;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import tbrugz.sqldump.datadump.DataDumpUtils;
import tbrugz.sqldump.util.CategorizedOut;
import tbrugz.sqldump.util.SQLUtils;

/*
 * TODOne: String: option to use compareToCaseInsensitive
 */
public class ResultSetDiff {

	static final Log log = LogFactory.getLog(ResultSetDiff.class);
	
	//XXX: option to set 'logEachXLoops' 
	int logEachXLoops = 1000;
	
	long limit = 0;
	boolean useCaseInsensitive = true; //databases, by default, uses case insensitive order
	
	int identicalRowsCount, updateCount, dumpCount, deleteCount, sourceRowCount, targetRowCount;
	
	//XXX: add property for dumpInserts, dumpUpdates & dumpDeletes
	boolean dumpInserts = true,
		dumpUpdates = true,
		dumpDeletes = true;
	
	public void diff(ResultSet source, ResultSet target, String schemaName, String tableName, List<String> keyCols,
			List<DiffSyntax> dss, String coutPattern) throws SQLException, IOException {
		diff(source, target, schemaName, tableName, keyCols, dss, coutPattern, null);
	}

	@Deprecated
	public void diff(ResultSet source, ResultSet target, String tableName, List<String> keyCols,
			DiffSyntax ds, Writer singleWriter) throws SQLException, IOException {
		List<DiffSyntax> dss = new ArrayList<DiffSyntax>(); dss.add(ds);
		diff(source, target, null, tableName, keyCols, dss, null, singleWriter);
	}
	
	public void diff(ResultSet source, ResultSet target, String schemaName, String tableName, List<String> keyCols,
			DiffSyntax ds, Writer singleWriter) throws SQLException, IOException {
		List<DiffSyntax> dss = new ArrayList<DiffSyntax>(); dss.add(ds);
		diff(source, target, schemaName, tableName, keyCols, dss, null, singleWriter);
	}
	
	//XXX: add schemaName ?
	@SuppressWarnings("rawtypes")
	void diff(ResultSet source, ResultSet target, String schemaName, String tableName, List<String> keyCols,
			List<DiffSyntax> dss, String coutPattern, Writer singleWriter) throws SQLException, IOException {
		boolean readSource = true;
		boolean readTarget = true;
		boolean hasNextSource = true;
		boolean hasNextTarget = true;
		List<Comparable> sourceVals = null;
		List<Comparable> targetVals = null;
		final String fullTableName = (schemaName!=null?schemaName+".":"")+tableName;
		
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
			if(keyIndexes[i]<0) {
				throw new IllegalArgumentException("key column not found: "+key);
			}
		}
		log.debug("[table="+fullTableName+"] key cols: "+keyCols);
		
		Map<DiffSyntax, DataDumpUtils.SyntaxCOutCallback> dscbs = new HashMap<DiffSyntax, DataDumpUtils.SyntaxCOutCallback>();
		Map<DiffSyntax, CategorizedOut> dscouts = new HashMap<DiffSyntax, CategorizedOut>();
		
		for(DiffSyntax ds: dss) {
			DataDumpUtils.SyntaxCOutCallback cb = new DataDumpUtils.SyntaxCOutCallback(ds);
			dscbs.put(ds, cb);
			if(singleWriter!=null) {
				dscouts.put(ds, new CategorizedOut(singleWriter, cb));
			}
			else {
				dscouts.put(ds, new CategorizedOut(coutPattern, cb));
			}
			ds.initDump(schemaName, tableName, keyCols, md);
		}
		//Writer w = new PrintWriter(System.out); //XXXxx: change to COut ? - [schemaname](?), [tablename], [changetype]
		identicalRowsCount = updateCount = dumpCount = deleteCount = sourceRowCount = targetRowCount = 0;
		long count = 0;
		boolean loggedLastLine = false;

		//header fos writer-independent syntaxes
		//XXX: what about stateful syntaxes?
		for(DiffSyntax ds: dss) {
			if(ds.isWriterIndependent()) {
				ds.dumpHeader();
			}
		}
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
			catch(NullPointerException e) {
				log.error("error comparing rows: source="+sourceVals+" ; target="+targetVals+" ; [ex="+e+"]");
				//e.printStackTrace();
				return;
			}
			catch(ClassCastException e) {
				log.error("error comparing rows: source="+sourceVals+" ; target="+targetVals+" ; [ex="+e+"]");
				//e.printStackTrace();
				return;
			}
			
			if(compare==0) {
				//same key
				readSource = readTarget = true;
				boolean updated = false;
				if(dumpUpdates) {
				for(DiffSyntax ds: dss) {
					//last 'updated' that counts...
					//TODO: compare (equals) should not be dumpSyntax responsability... or should it? no! so that 'getCategorizedWriter' may not be called
					CategorizedOut cout = dscouts.get(ds);
					updated = ds.dumpUpdateRowIfNotEquals(source, target, count, cout.getCategorizedWriter("", tableName, "update", ds.getDefaultFileExtension()));
				}
				log.debug("update? "+sourceVals+" / "+targetVals+(updated?" [updated]":"")+" // "+hasNextSource+"/"+hasNextTarget);
				}
				if(updated) { updateCount++; }
				else { identicalRowsCount++; }
			}
			else if(compare<0) {
				readSource = true; readTarget = false;
				if(hasNextSource) {
					if(dumpDeletes) {
					log.debug("delete: ->"+sourceVals+" / "+targetVals+" // "+hasNextSource+"/"+hasNextTarget);
					for(DiffSyntax ds: dss) {
						CategorizedOut cout = dscouts.get(ds);
						ds.dumpDeleteRow(source, count, cout.getCategorizedWriter("", tableName, "delete", ds.getDefaultFileExtension()));
					}
					}
					deleteCount++;
				}
				else {
					readSource = false; readTarget = true;
					if(dumpInserts) {
					log.debug("insert: "+sourceVals+" / ->"+targetVals+" // "+hasNextSource+"/"+hasNextTarget);
					for(DiffSyntax ds: dss) {
						CategorizedOut cout = dscouts.get(ds);
						ds.dumpRow(target, count, cout.getCategorizedWriter("", tableName, "insert", ds.getDefaultFileExtension()));
					}
					}
					dumpCount++;
				}
			}
			else {
				readSource = false; readTarget = true;
				if(hasNextTarget) {
					if(dumpInserts) {
					log.debug("insert: "+sourceVals+" / ->"+targetVals+" // "+hasNextSource+"/"+hasNextTarget);
					for(DiffSyntax ds: dss) {
						CategorizedOut cout = dscouts.get(ds);
						ds.dumpRow(target, count, cout.getCategorizedWriter("", tableName, "insert", ds.getDefaultFileExtension()));
					}
					}
					dumpCount++;
				}
				else {
					readSource = true; readTarget = false;
					if(dumpDeletes) {
					log.debug("delete: ->"+sourceVals+" / "+targetVals+" // "+hasNextSource+"/"+hasNextTarget);
					for(DiffSyntax ds: dss) {
						CategorizedOut cout = dscouts.get(ds);
						ds.dumpDeleteRow(source, count, cout.getCategorizedWriter("", tableName, "delete", ds.getDefaultFileExtension()));
					}
					}
					deleteCount++;
				}
				
				if(!hasNextTarget) { readSource = true; readTarget = false; }
			}
			
			count++;
			
			if( (logEachXLoops>0) && (count>0) && (count%logEachXLoops==0) ) { 
				log.info("[table="+fullTableName+"] "+count+" rows compared ; stats: "+getCompactStats());
				loggedLastLine = true;
			}
			else {
				loggedLastLine = false;
			}
			
			if(limit>0 && count>=limit) {
				log.info("limit reached: "+limit+" [table="+fullTableName+"]");
				break;
			}
		}
		if(!loggedLastLine) {
			log.info("[table="+fullTableName+"] "+count+" rows compared ; stats: "+getCompactStats());
		}
		
		//footer for writer-independent syntaxes
		//XXX: what about stateful syntaxes?
		for(DiffSyntax ds: dss) {
			if(ds.isWriterIndependent()) {
				ds.dumpFooter(sourceRowCount); //XXX: sourceRowCount is the best for ds.dumpFooter(<count>, writer)?
			}
			else {
				CategorizedOut cout = dscouts.get(ds);
				Iterator<Writer> it = cout.getAllOpenedWritersIterator();
				while(it.hasNext()) {
					Writer w = it.next();
					ds.dumpFooter(sourceRowCount, w);
					w.close();
				}
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
	int compareVals(List<Comparable> vals1, List<Comparable> vals2, int[] keyIndexes) {
		int comp = 0;
		for(int i=0;i<keyIndexes.length; i++) {
			try {
				Comparable v1 = vals1.get(keyIndexes[i]);
				Comparable v2 = vals2.get(keyIndexes[i]);
				comp = v1!=null && v2!=null ? ((useCaseInsensitive && v1 instanceof String)? ((String)v1).compareToIgnoreCase((String)v2) : v1.compareTo(v2)) :
					 ( v1==null && v2==null ? 0 :
					 ( v1==null ? -1 : 1 ) ); // "NULLs last", right?
				
				if(v1==null && v2!=null) {
					log.debug("compareVals: v1 null: v2=="+v2+" ; comp = "+comp);
					//log.info("compareVals: v1 null: v2=="+v2+" ; v2.compareTo(v1): "+v2.compareTo(v1));
				}
				if(v1!=null && v2==null) {
					log.debug("compareVals: v1=="+v1+": v2 null ; comp = "+comp);
					//log.info("compareVals: v1 =="+v1+": v2 null ; v1.compareTo(v2): "+v1.compareTo(v2));
				}
				/* comp = v1!=null ? v1.compareTo(v2) :
					 ( v2!=null ? v2.compareTo(v1) : 0 ); */
				//comp = vals1.get(keyIndexes[i]).compareTo(vals2.get(keyIndexes[i]));
			}
			catch(NullPointerException e) {
				log.warn("[NPE;i="+i+"] key idx="+keyIndexes[i]+" ; v1="+vals1.get(keyIndexes[i])+" ; v2="+vals2.get(keyIndexes[i])+" [ex: "+e+"]");
			}
			catch(ClassCastException e) {
				log.warn("[CCE;i="+i+"] key idx="+keyIndexes[i]+" ; v1="+vals1.get(keyIndexes[i])+" ; v2="+vals2.get(keyIndexes[i])+" [ex: "+e+"]");
			}
			if(comp!=0) {
				//log.debug("compareVals: comp = "+comp+"; v1="+vals1.get(keyIndexes[i])+" ; v2="+vals2.get(keyIndexes[i]));
				return comp;
			}
		}
		//log.debug("compareVals-z: comp = "+comp);
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
}
