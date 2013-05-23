package tbrugz.sqldump.processors;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.regex.Matcher;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import tbrugz.sqldump.datadump.DataDump;
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
 */
public class SchemaPartitionDataDump extends AbstractSQLProc {

	static final Log log = LogFactory.getLog(SchemaPartitionDataDump.class);
	
	//prefix
	static final String SPDD_PROP_PREFIX = "sqldump.schemapartitiondd";
	
	//generic props
	static final String PROP_SPDD_STARTTABLES = SPDD_PROP_PREFIX+".starttables";
	static final String PROP_SPDD_STOPTABLES = SPDD_PROP_PREFIX+".stoptables";
	static final String PROP_SPDD_ORDERBYPK = SPDD_PROP_PREFIX+".orderbypk";
	static final String PROP_SPDD_EXPORTEDKEYS = SPDD_PROP_PREFIX+".exportedkeys";
	//XXX add max(recursion)level for exported FKs?
	static boolean addAlias = true;
	static boolean addSQLremarks = true;
	
	String quote = DBMSResources.instance().getIdentifierQuoteString();
	//StringDecorator quoter = new StringDecorator.StringQuoterDecorator(quote);
	StringDecorator quoter = null; //XXX add prop?
	
	List<String> startTables = null;
	List<String> stopTables = null; //XXX for exported FKs only?
	boolean orderByPK = true;
	Boolean exportedKeys = null;

	Map<String, List<Query4CDD>> queries4dump = new LinkedHashMap<String, List<Query4CDD>>();
	//Map<String, List<Query4CDD>> queries4dump = new NonNullGetMap<String, List<Query4CDD>>(new LinkedHashMap<String, List<Query4CDD>>(), ArrayList<Query4CDD>.class);
	Map<String, Constraint> tablePKs = new HashMap<String, Constraint>();
	
	@Override
	public void setProperties(Properties prop) {
		super.setProperties(prop);
		startTables = Utils.getStringListFromProp(prop, PROP_SPDD_STARTTABLES, ",");
		stopTables = Utils.getStringListFromProp(prop, PROP_SPDD_STOPTABLES, ",");
		orderByPK = Utils.getPropBoolean(prop, PROP_SPDD_ORDERBYPK, orderByPK);
		exportedKeys = Utils.getPropBoolean(prop, PROP_SPDD_EXPORTEDKEYS, null);
	}

	@Override
	public void process() {
		int count=0;
		for(String tablename: startTables) {
			Table t = DBIdentifiable.getDBIdentifiableByName(model.getTables(), tablename);
			if(t==null) {
				log.warn("table '"+tablename+"' not found");
				continue;
			}
			
			String filter = prop.getProperty(SPDD_PROP_PREFIX+".filter@"+t.getName());
			dumpTable(t, null, filter, exportedKeys);
			count++;
		}
		//XXX: warn for filters not used (for tables not in starttables)

		log.info("dumping tables [#"+dumpedTablesList.size()+"]: "+dumpedTablesList);
		
		DataDump dd = new DataDump();
		List<String> dumpedTables = new ArrayList<String>();
		try {
			for(String tname: queries4dump.keySet()) {
				List<Query4CDD> qs = queries4dump.get(tname);
				if(qs.size()==0) {
					log.warn("0 queries for table '"+tname+"'");
				}
				else if(qs.size()==1) {
					Query4CDD q4cdd = qs.get(0);
					String sql = q4cdd.sql;
					if(orderByPK) {
						Constraint pk = tablePKs.get(tname);
						sql = addOrderBy(sql, pk);
					}
					log.debug("simple-sql["+tname+"]:\n"+sql);
					dd.runQuery(conn, sql, null, prop, tname, tname);
					dumpedTables.add(tname);
				}
				else {
					log.debug("many queries for table '"+tname+"': "+qs.size());
					String setOperation = null;
					if(areAllImported(qs)) {
						//union
						setOperation = "union";
					}
					else if(areAllExported(qs)) {
						//intersect
						setOperation = "intersect";
					}
					else {
						//XXX what??
						//continue;
						setOperation = "intersect";
						log.warn("many queries for table '"+tname+"' ["+qs.size()+"] with different import/export property - defaulting to '"+setOperation+"' operation");

						/*//dump 1st?
						dd.runQuery(conn, qs.get(0).sql, null, prop, tname, tname);
						dumpedTables.add(tname);
						continue;*/
					}
					
					StringBuilder sb = new StringBuilder();
					//XXX: test for equality between sql parts?
					for(int i=0;i<qs.size();i++) {
						if(i>0) { sb.append("\n"+setOperation+"\n"); }
						Query4CDD q = qs.get(i);
						sb.append(q.sql);
					}
					Constraint pk = tablePKs.get(tname);
					if(orderByPK && pk!=null) {
						addOrderBy(sb, pk);
					}
					
					log.debug("join-sql["+tname+"]:\n"+sb.toString());
					dd.runQuery(conn, sb.toString(), null, prop, tname, tname);
					dumpedTables.add(tname);
				}
			}
		} catch (Exception e) {
			log.warn("error running query: "+e);
			if(failonerror) {
				throw new ProcessingException(e);
			}
		}
		
		log.info("partitioned dump done [#start points="+count+"]");
		log.info("dumped tables [#"+dumpedTables.size()+"]: "+dumpedTables);
	}

	List<String> dumpedTablesList = new ArrayList<String>();
	Set<String> dumpedFKs = new LinkedHashSet<String>();
	
	void dumpTable(final Table t, final List<FK> fks, final String filter, final Boolean followExportedKeys) {
		if(fks!=null) {
			//prevents infinite recursion... remove this?
			if(!dumpedFKs.add(fks.get(0).toStringFull())) {
				return;
			}
			
			//TODO: detect cycles (source of 1st fk equals target of last) - use CTE (common table expressions)?
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
		if(stopTables.contains(t.getName())) {
			return;
		}
		
		//get filter/sql
		if(fks==null) {
			log.info("start table '"+t.getName()+"'"
					+(filter!=null?" [filter: "+filter+"]":""));
		}
		String join = getSQL(t,fks,filter);
		
		//importedKeys recursive dump
		procFKs4Dump(t, fks, filter, false);
		
		//do dump
		String sql = "select distinct "+t.getName()+".* from "+t.getName()
				+(join!=null?join:"");
		Constraint ctt = t.getPKConstraint();
		if(ctt!=null) {
			tablePKs.put(t.getName(), ctt);				
		}
		log.debug("sql[followEx="+followExportedKeys+"]: "+sql);
		Query4CDD qd = new Query4CDD(sql, followExportedKeys);
		List<Query4CDD> qs = queries4dump.get(t.getName());
		if(qs==null) {
			qs = new ArrayList<Query4CDD>();
			queries4dump.put(t.getName(), qs);
		}
		qs.add(qd);
		dumpedTablesList.add(t.getName());
		
		//exportedKeys recursive dump
		if(followExportedKeys!=null && followExportedKeys) {
			procFKs4Dump(t, fks, filter, true);
		}
	}
	
	void procFKs4Dump(final Table t, final List<FK> fks, final String filter, final boolean exportedKeys) {
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
					log.warn("fk "+fk+" already added...");
					continue;
				}
				if(fk.getFkTable().equals(fk.getPkTable())) {
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
							log.warn("existing FK["+t.getName()+"]: "+xfk+" <newFKs:"+newFKs+"> <origFKs:"+fks+">");
							//XXX continue OUTER; //or just if "last" FK?
						}
						else {
							newFKs.add(xfk);
						}
					}
					//newFKs.addAll(fks);
				}
				//if(exportedKeys) {newFKs.add(fk);}
				dumpTable(pkt, newFKs, filter, exportedKeys);
			}
		}
	}
	
	static String getSQL(final Table t, final List<FK> fks, String filter) {
		if(addAlias) {
			return getSQLAlias(t, fks, filter);
		}
		else {
			return getSQLNoAlias(t, fks, filter);
		}
	}
	
	static String getSQLAlias(final Table t, final List<FK> fks, final String filter) {
		if(fks==null) return null;
		
		StringBuilder sb = new StringBuilder();
		String lastTable = t.getName();
		String lastAlias = t.getName();
		if(addSQLremarks) { sb.append("\n/* "+fks+" */"); }
		for(int i=0;i<fks.size();i++) {
			FK fk = fks.get(i);
			boolean joinWithFKT = lastTable.equals(fk.getPkTable());
			String tableJoin = joinWithFKT?fk.getFkTable():fk.getPkTable();
			String alias = "A"+i;
			if(tableJoin.equals(t.getName())) {
				log.warn("-->>>>> autojoin '"+tableJoin+"': "+sb.toString());
				continue;
			}
			sb.append("\ninner join "+tableJoin+" "+alias);
			//sb.append("\ninner join "+tableJoin);
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
			String newFilter = filter.replaceAll(DataDump.PATTERN_TABLENAME_FINAL, Matcher.quoteReplacement(lastAlias));
			sb.append("\nwhere "+newFilter);
		}
		return sb.toString();
	}

	static String getSQLNoAlias(final Table t, final List<FK> fks, String filter) {
		if(fks==null) return null;
		
		StringBuilder sb = new StringBuilder();
		String lastTable = t.getName();
		if(addSQLremarks) { sb.append("\n/* "+fks+" */"); }
		for(int i=0;i<fks.size();i++) {
			FK fk = fks.get(i);
			String tableJoin = (lastTable.equals(fk.getPkTable())?fk.getFkTable():fk.getPkTable());
			if(tableJoin.equals(t.getName())) {
				log.warn("-->>>>> autojoin '"+tableJoin+"': "+sb.toString());
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
			//sb.append("\nwhere "+lastTable+"."+filter);
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
			log.info("allImported[all-should-be-false]? exported="+sb.toString());
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
			log.info("allExported[all-should-be-true]? exported="+sb.toString());
		}
		return ret;
	}

	static void addOrderBy(StringBuilder sb, Constraint pk) {
		if(pk==null) { return; }
		
		sb.insert(0, "select * from (\n");
		sb.append("\n) order by "+Utils.join(pk.uniqueColumns, ", ", null));
	}

	static String addOrderBy(String s, Constraint pk) {
		if(pk==null) { return s; }
		
		StringBuilder sb = new StringBuilder();
		sb.append(s);
		addOrderBy(sb, pk);
		return sb.toString();
	}
	
}
