package tbrugz.sqldump.processors;

import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.regex.Matcher;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import tbrugz.sqldump.datadump.DataDump;
import tbrugz.sqldump.dbmodel.Column;
import tbrugz.sqldump.dbmodel.Constraint;
import tbrugz.sqldump.dbmodel.DBIdentifiable;
import tbrugz.sqldump.dbmodel.FK;
import tbrugz.sqldump.dbmodel.Table;
import tbrugz.sqldump.def.AbstractSQLProc;
import tbrugz.sqldump.def.DBMSResources;
import tbrugz.sqldump.def.ProcessingException;
import tbrugz.sqldump.util.StringDecorator;
import tbrugz.sqldump.util.Utils;

class Query4CDD {
	//final String tableName;
	final String sql;
	final Boolean exported;
	
	public Query4CDD(String sql, Boolean exported) {
		//this.tableName = tableName;
		this.exported = exported;
		this.sql = sql;
	}
}

/*
 * possible names: CascadingDataDump, SchemaPartitionDataDump, MasterDetailDataDump
 * 
 * XXX: what if multiple paths (from multiple start points) reach the same table?
 * union? intersect? save queries for later execution...
 * - if multiple filters, intersect
 * - if table is parent of multiple other tables, union 
 * XXXdone: go into all directions (follow FK-IM after FM-EX - DONE)?
 * ~XXX: option to follow FM-EX after FK-IM?
 * XXX?: first follow all exported, then use generated 'intersect' queries to grab imported/parent tables with 'union' queries
 * TODO: add test case!
 * TODO: 'exported' property for each start-table
 */
public class CascadingDataDump extends AbstractSQLProc {

	static final Log log = LogFactory.getLog(CascadingDataDump.class);
	
	//prefix
	static final String CDD_PROP_PREFIX = "sqldump.cascadingdd";
	
	//generic props
	static final String PROP_CDD_STARTTABLES = CDD_PROP_PREFIX+".starttables";
	static final String PROP_CDD_STOPTABLES = CDD_PROP_PREFIX+".stoptables";
	static final String PROP_CDD_NOEXPORTTABLES = "sqldump.cascadingdd.noexporttables";
	static final String PROP_CDD_ORDERBYPK = CDD_PROP_PREFIX+".orderbypk";
	static final String PROP_CDD_EXPORTEDKEYS = CDD_PROP_PREFIX+".exportedkeys";
	//XXX add max(recursion)level for exported FKs?
	
	static final String[] nonDistinctableColumnTypesArr = { "BLOB", "CLOB" };
	static final List<String> nonDistinctableColumnTypes = Arrays.asList(nonDistinctableColumnTypesArr);
	
	static boolean addAlias = true;
	static boolean addSQLremarks = true;
	
	String quote = DBMSResources.instance().getIdentifierQuoteString();
	//StringDecorator quoter = new StringDecorator.StringQuoterDecorator(quote);
	StringDecorator quoter = null; //XXX add prop?
	
	List<String> startTables = null;
	List<String> stopTables = null; //XXX for exported FKs only?
	List<String> noExportTables = null; //do not follow exported keys for these tables
	boolean orderByPK = true;
	boolean exportedKeys = false;

	Map<String, List<Query4CDD>> queries4dump = new LinkedHashMap<String, List<Query4CDD>>();
	//Map<String, List<Query4CDD>> queries4dump = new NonNullGetMap<String, List<Query4CDD>>(new LinkedHashMap<String, List<Query4CDD>>(), ArrayList<Query4CDD>.class);
	Map<String, Constraint> tablePKs = new HashMap<String, Constraint>();
	
	@Override
	public void setProperties(Properties prop) {
		super.setProperties(prop);
		startTables = Utils.getStringListFromProp(prop, PROP_CDD_STARTTABLES, ",");
		stopTables = Utils.getStringListFromProp(prop, PROP_CDD_STOPTABLES, ",");
		noExportTables = Utils.getStringListFromProp(prop, PROP_CDD_NOEXPORTTABLES, ",");
		if(noExportTables==null) { noExportTables = new ArrayList<String>(); }
		orderByPK = Utils.getPropBool(prop, PROP_CDD_ORDERBYPK, orderByPK);
		exportedKeys = Utils.getPropBool(prop, PROP_CDD_EXPORTEDKEYS, exportedKeys);
	}

	@Override
	public void process() {
		if(startTables==null) {
			String message = "no start-tables defined [prop '"+PROP_CDD_STARTTABLES+"']";
			log.warn(message);
			if(failonerror) { throw new ProcessingException(message); }
			return;
		}
		
		int count=0;
		for(String tablename: startTables) {
			Table t = DBIdentifiable.getDBIdentifiableByName(model.getTables(), tablename);
			if(t==null) {
				log.warn("table '"+tablename+"' not found");
				continue;
			}
			
			String filter = prop.getProperty(CDD_PROP_PREFIX+".filter@"+t.getName());
			dumpTable(t, null, t, filter, null, false);
			count++;
		}
		//XXX: warn for filters not used (for tables not in starttables)

		log.info("dumping tables [#"+dumpedTablesList.size()+"]: "+dumpedTablesList);
		
		List<String> dumpedTables = new ArrayList<String>();
		try {
			dumpQueries(dumpedTables);
		} catch (Exception e) {
			log.warn("error running query: "+e);
			if(failonerror) {
				throw new ProcessingException(e);
			}
		}
		
		log.info("cascading datadump done [#start points="+count+"]");
		log.info("dumped tables [#"+dumpedTables.size()+"]: "+dumpedTables);
	}

	List<String> dumpedTablesList = new ArrayList<String>();
	//Set<String> dumpedFKs = new LinkedHashSet<String>();
	
	void dumpTable(final Table t, final List<FK> fks, final Table filterTable, final String filter, final Boolean followExportedKeys, final boolean doIntersect) {
		if(fks!=null) {
			//prevents infinite recursion... remove this?
			/*if(!dumpedFKs.add(fks.get(0).toStringFull())) {
				return;
			}*/
			
			//TODO: detect cycles (source of 1st fk equals target of last, if all are imported or all exported) - use CTE (common table expressions)?
			/*
			FK fkOne = fks.get(0);
			FK fkLast = fks.get(fks.size()-1);
			if(fks.size()>1 && (t.getName().equals(fkLast.getFkTable()) || t.getName().equals(fkLast.getPkTable())) ) {
				log.warn("loop detected !! [fEx="+followExportedKeys+",#="+fks.size()+"]! "+fks+" fl:"+fkLast);
				return;
			}
			
			for(int i=0;i<fks.size();i++) {
				FK fkX = fks.get(i);
				if(followExportedKeys && fkOne.getPkTable().equals(fkX.getFkTable())) {
					log.warn("loop detected[fEx="+followExportedKeys+",#="+fks.size()+"]! "+fks+" f1:"+fkOne+" fl:"+fkX);
					return;
				}
				if(!followExportedKeys && fkOne.getFkTable().equals(fkX.getPkTable())) {
					log.warn("loop detected[fEx="+followExportedKeys+",#="+fks.size()+"]! "+fks+" f1:"+fkOne+" fl:"+fkX);
					return;
				}
			}*/
		}
		//stop at starting tables if comming from 'importing' FK
		if(fks!=null && startTables.contains(t.getName()) && followExportedKeys!=null && !followExportedKeys) {
			//add empty list to keep proper dump order
			if(queries4dump.get(t.getName())==null) {
				queries4dump.put(t.getName(), new ArrayList<Query4CDD>());
			}
			return;
		}
		if(stopTables!=null && stopTables.contains(t.getName())) {
			//XXX: only stop at stopTables when followExportedKeys is true?
			return;
		}
		
		//get filter/sql
		if(fks==null) {
			log.info("start table '"+t.getName()+"'"
					+(filter!=null?" [filter: "+filter+"]":"")
					+" [follow-exported? "+ (exportedKeys && !noExportTables.contains(t.getName()))+"]");
		}
		String join = getSQL(t,fks,filterTable,filter);
		
		//importedKeys recursive dump
		procFKs4Dump(t, fks, filterTable, filter, false, (followExportedKeys!=null && followExportedKeys)?true:doIntersect);
		
		//add query to dump list
		//FIXedME: not all columns can be 'distincted' - select only those that can
		String sql = "select distinct "+getProjectionForDistinct(t)+" from "+t.getName()
				+(join!=null?join:
					(filter!=null?
						(addSQLremarks?"\n/* original filter start table */":"")+
						"\nwhere "+filter.replaceAll(DataDump.PATTERN_TABLENAME_FINAL, Matcher.quoteReplacement(t.getName()))
						:"")
				);
		Constraint ctt = t.getPKConstraint();
		if(ctt!=null) {
			tablePKs.put(t.getName(), ctt);				
		}
		log.debug("sql[followEx="+followExportedKeys+";doIntersect="+doIntersect+"]: "+sql);
		Query4CDD qd = new Query4CDD(sql, doIntersect?(Boolean)doIntersect:followExportedKeys);
		List<Query4CDD> qs = queries4dump.get(t.getName());
		if(qs==null) {
			qs = new ArrayList<Query4CDD>();
			queries4dump.put(t.getName(), qs);
		}
		qs.add(qd);
		dumpedTablesList.add(t.getName());
		
		//exportedKeys recursive dump
		if(exportedKeys && (followExportedKeys==null || followExportedKeys)) {
			if(!noExportTables.contains(t.getName())) {
				procFKs4Dump(t, fks, filterTable, filter, true, doIntersect);
			}
			else {
				log.debug("not following exported keys for table '"+t.getName()+"'");
			}
		}
	}
	
	void procFKs4Dump(final Table t, final List<FK> fks, final Table filterTable, final String filter, final boolean exportedKeys, final boolean doIntersect) {
		List<FK> fki = null;
		if(exportedKeys) {
			fki = DBIdentifiable.getExportedKeys(t, model.getForeignKeys());
			log.debug("fks-ex '"+t.getName()+"': "+fki);
		}
		else {
			fki = DBIdentifiable.getImportedKeys(t, model.getForeignKeys());
			log.debug("fks-im '"+t.getName()+"': "+fki);
		}
		
		if(fki!=null) {
			//OUTER:
			for(FK fk: fki) {
				if(fks!=null && fks.get(0).equals(fk)) {
					//log.warn("fk "+fk+" already added...");
					continue;
				}
				if(fk.getFkTable().equals(fk.getPkTable())) {
					//XXX: dump self-relationship - warn once for each FK?
					log.warn("self-relationship [loop] detected: "+fk.toStringFull()+" [not yet implemented]");
					continue;
				}
				
				String table = exportedKeys?fk.getFkTable():fk.getPkTable();
				String schema = exportedKeys?fk.getFkTableSchemaName():fk.getPkTableSchemaName();
				Table pkt = DBIdentifiable.getDBIdentifiableBySchemaAndName(model.getTables(), schema, table);
				List<FK> newFKs = new ArrayList<FK>();
				newFKs.add(fk);
				//if(!exportedKeys) {newFKs.add(fk);}
				if(fks!=null) {
					for(FK xfk: fks) {
						if(fk.equals(xfk)) {
							log.debug("existing FK["+t.getName()+"]: "+xfk+" <newFKs:"+newFKs+"> <origFKs:"+fks+">");
							//XXX continue OUTER; //or just if "last" FK?
						}
						else {
							newFKs.add(xfk);
						}
					}
					//newFKs.addAll(fks);
				}
				//if(exportedKeys) {newFKs.add(fk);}
				try {
					dumpTable(pkt, newFKs, filterTable, filter, exportedKeys, doIntersect);
				}
				catch(StackOverflowError err) {
					try {
						System.err.println("CascadingDataDump: stack overflow error: table: "+t.getName());
					}
					catch (Error e) {}
					throw err;
				}
			}
		}
	}
	
	void dumpQueries(List<String> dumpedTables) throws SQLException, IOException {
		DataDump dd = new DataDump();
		
		//XXX: option to invert 'queries4dump.keySet()' order [ Collections.reverse(<list>)? ] - for delete?
		for(String tname: queries4dump.keySet()) {
			List<Query4CDD> qs = queries4dump.get(tname);
			Constraint pk = tablePKs.get(tname);
			if(qs.size()==0) {
				log.warn("0 queries for table '"+tname+"'");
			}
			else if(qs.size()==1) {
				Query4CDD q4cdd = qs.get(0);
				String sql = q4cdd.sql;
				if(orderByPK) {
					sql = addOrderBy(sql, pk);
				}
				log.debug("simple-sql["+tname+"]:\n"+sql);
				dd.runQuery(conn, sql, null, prop, tname, tname, null, pk!=null?pk.getUniqueColumns():null);
				dumpedTables.add(tname);
			}
			else {
				log.debug("many queries for table '"+tname+"': "+qs.size());
				String setOperation = null;
				StringBuilder sb = new StringBuilder();
				if(areAllImported(qs)) {
					//union
					setOperation = "union";
				}
				else if(areAllExported(qs)) {
					//intersect
					setOperation = "intersect";
				}
				else {
					boolean intersectPriority = true;
					log.warn("many queries for table '"+tname+"' ["+qs.size()+"] with different import/export property [using '"+(intersectPriority?"intersect":"union")+"' priority]");

					//XXX (i1 intersect i2 intersect i3) union u1 union u2 [intersectPriority == true]
					// or (u1 union u2) intersect i1 intersect i2 intersect i3 [intersectPriority == false] ?
					
					String oper = null;
					int count = 0;

					//1st operation
					if(intersectPriority) {
					oper = "intersect";
					} else {
					oper = "union";
					}
					for(int i=0;i<qs.size();i++) {
						Query4CDD q = qs.get(i);
						if(intersectPriority) {
						if(q.exported!=null && !q.exported) { continue; }
						}
						else {
						if(q.exported==null || q.exported) { continue; }
						}
						if(count>0) { sb.append("\n"+oper+"\n"); }
						if(addSQLremarks) { sb.append("/* ex="+q.exported+" */\n"); }
						sb.append(q.sql);
						count++;
					}
					
					if(count>1) {
						sb.insert(0, "(\n");
						sb.append("\n)");
					}

					//2nd operation
					if(intersectPriority) {
					oper = "union";
					} else {
					oper = "intersect";
					}
					for(int i=0;i<qs.size();i++) {
						Query4CDD q = qs.get(i);
						if(intersectPriority) {
						if(q.exported==null || q.exported) { continue; }
						}
						else {
						if(q.exported!=null && !q.exported) { continue; }
						}
						if(count>0) { sb.append("\n"+oper+"\n"); }
						if(addSQLremarks) { sb.append("/* ex="+q.exported+" */\n"); }
						sb.append(q.sql);
						count++;
					}
				}

				if(setOperation!=null) {
					//XXX: test for equality between sql parts?
					for(int i=0;i<qs.size();i++) {
						if(i>0) { sb.append("\n"+setOperation+"\n"); }
						Query4CDD q = qs.get(i);
						if(addSQLremarks) { sb.append("/* ex="+q.exported+" */\n"); }
						sb.append(q.sql);
					}
				}
				
				if(orderByPK && pk!=null) {
					addOrderBy(sb, pk);
				}
				
				log.debug("join-sql["+tname+"]:\n"+sb.toString());
				dd.runQuery(conn, sb.toString(), null, prop, tname, tname, null, pk!=null?pk.getUniqueColumns():null);
				dumpedTables.add(tname);
			}
		}
	}
	
	static String getSQL(final Table t, final List<FK> fks, final Table filterTable, String filter) {
		if(addAlias) {
			return getSQLAlias(t, fks, filterTable, filter);
		}
		else {
			return getSQLNoAlias(t, fks, filterTable, filter);
		}
	}
	
	static String getSQLAlias(final Table t, final List<FK> fks, final Table filterTable, final String filter) {
		if(fks==null) { return null; }
		
		StringBuilder sb = new StringBuilder();
		String lastTable = t.getName();
		String lastAlias = t.getName();
		String filterAlias = filterTable.getName();
		if(addSQLremarks) { sb.append("\n/* "+fks+" */"); }
		for(int i=0;i<fks.size();i++) {
			FK fk = fks.get(i);
			boolean joinWithFKT = lastTable.equals(fk.getPkTable());
			String tableJoin = joinWithFKT?fk.getFkTable():fk.getPkTable();
			if(tableJoin.equals(t.getName())) {
				log.debug("--> autojoin '"+tableJoin+"': "+sb.toString());
				//continue;
			}
			String alias = "A"+i;
			if(tableJoin.equals(filterTable.getName())) {
				filterAlias = alias;
			} 
			sb.append("\ninner join "+tableJoin+" "+alias);
			for(int ci=0;ci<fk.getPkColumns().size();ci++) {
				if(ci==0) { sb.append(" on "); }
				else { sb.append(" and "); }
				//sb.append(fk.getPkTable()+"."+fk.getPkColumns().get(ci)+" = "+fk.getFkTable()+"."+fk.getFkColumns().get(ci));
				sb.append( (joinWithFKT?lastAlias:alias) +"."+fk.getPkColumns().get(ci));
				sb.append(" = ");
				sb.append( (joinWithFKT?alias:lastAlias) +"."+fk.getFkColumns().get(ci));
			}
			lastTable = tableJoin;
			lastAlias = alias;
		}
		if(filter!=null) {
			String newFilter = filter.replaceAll(DataDump.PATTERN_TABLENAME_FINAL, Matcher.quoteReplacement(filterAlias));
			sb.append("\nwhere "+newFilter);
		}
		return sb.toString();
	}

	@Deprecated
	static String getSQLNoAlias(final Table t, final List<FK> fks, final Table filterTable, String filter) {
		if(fks==null) { return null; }
		
		StringBuilder sb = new StringBuilder();
		String lastTable = t.getName();
		if(addSQLremarks) { sb.append("\n/* "+fks+" */"); }
		for(int i=0;i<fks.size();i++) {
			FK fk = fks.get(i);
			String tableJoin = (lastTable.equals(fk.getPkTable())?fk.getFkTable():fk.getPkTable());
			if(tableJoin.equals(t.getName())) {
				//log.warn("-->>>>> autojoin '"+tableJoin+"': "+sb.toString());
				continue;
			}
			sb.append("\ninner join "+tableJoin);
			for(int ci=0;ci<fk.getPkColumns().size();ci++) {
				if(ci==0) { sb.append(" on "); }
				else { sb.append(" and "); }
				sb.append(fk.getPkTable()+"."+fk.getPkColumns().get(ci) +" = "+ fk.getFkTable()+"."+fk.getFkColumns().get(ci));
			}
			lastTable = tableJoin;
		}
		if(filter!=null) {
			String newFilter = filter.replaceAll(DataDump.PATTERN_TABLENAME_FINAL, Matcher.quoteReplacement(lastTable));
			sb.append("\nwhere "+newFilter);
		}
		return sb.toString();
	}
	
	String qualifiedColName(Table t, String col) {
		if(quoter==null) {
			return t.getName()+"."+col;
		}
		return quoter.get(t.getName())+"."+quoter.get(col);
	}
	
	static boolean areAllImported(List<Query4CDD> qs) {
		StringBuffer sb = new StringBuffer();
		boolean ret = true;
		for(Query4CDD q: qs) {
			sb.append(q.exported+", ");
			if(q.exported!=null && q.exported) {
				ret = false;
			}
			//if(q.exported==null || q.exported) { return false; }
		}
		if(!ret) {
			log.debug("allImported/union[all-should-be-false]? exported="+sb.toString());
		}
		return ret;
	}

	static boolean areAllExported(List<Query4CDD> qs) {
		StringBuffer sb = new StringBuffer();
		boolean ret = true;
		for(Query4CDD q: qs) {
			sb.append(q.exported+", ");
			if(q.exported!=null && !q.exported) {
				ret = false;
			}
			//if(q.exported==null || !q.exported) { return false; }
		}
		if(!ret) {
			log.debug("allExported/intersect[all-should-be-true]? exported="+sb.toString());
		}
		return ret;
	}

	static void addOrderBy(StringBuilder sb, Constraint pk) {
		if(pk==null) { return; }
		
		sb.insert(0, "select * from (\n");
		sb.append("\n) order by "+Utils.join(pk.getUniqueColumns(), ", ", null));
	}

	static String addOrderBy(String s, Constraint pk) {
		if(pk==null) { return s; }
		
		StringBuilder sb = new StringBuilder();
		sb.append(s);
		addOrderBy(sb, pk);
		return sb.toString();
	}
	
	static String getProjectionForDistinct(Table t) {
		String tname = t.getName();
		StringBuilder sb = new StringBuilder();
		int countDistinctableCols = 0;
		int countNonDistinctable = 0;
		for(Column c: t.getColumns()) {
			if(nonDistinctableColumnTypes.contains(c.type.toUpperCase())) {
				countNonDistinctable++;
			}
			else {
				if(countDistinctableCols!=0) { sb.append(", "); }
				sb.append(tname+"."+c.getName());
				countDistinctableCols++;
			}
		}
		if(countNonDistinctable>0) {
			return sb.toString();
		}
		return tname+".*";
	}
	
}
