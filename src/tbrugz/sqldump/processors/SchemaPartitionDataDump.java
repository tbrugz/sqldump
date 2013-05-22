package tbrugz.sqldump.processors;

import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Properties;
import java.util.Set;

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

/*
 * possible names: CascadingDataDump, SchemaPartitionDataDump, MasterDetailDataDump
 * 
 * XXX: what if multiple paths (from multiple start points) reach the same table?
 * union? intersect? save queries for later execution...
 * - if multiple filters, intersect
 * - if table is parent of multiple other tables, union 
 * XXXdone: go into all directions (follow FK-IM after FM-EX - DONE)?
 * XXX: option to follow FM-EX after FK-IM?
 */
public class SchemaPartitionDataDump extends AbstractSQLProc {

	static final Log log = LogFactory.getLog(SchemaPartitionDataDump.class);
	
	//prefix
	static final String SPDD_PROP_PREFIX = "sqldump.schemapartitiondd";
	
	//generic props
	static final String PROP_SPDD_STARTTABLES = SPDD_PROP_PREFIX+".starttables";
	static final String PROP_SPDD_STOPTABLES = SPDD_PROP_PREFIX+".stoptables";
	static final String PROP_SPDD_EXPORTEDKEYS = SPDD_PROP_PREFIX+".exportedkeys";
	//XXX add max(recursion)level for exported FKs?
	
	String quote = DBMSResources.instance().getIdentifierQuoteString();
	//StringDecorator quoter = new StringDecorator.StringQuoterDecorator(quote);
	StringDecorator quoter = null; //XXX add prop?
	
	List<String> startTables = null;
	List<String> stopTables = null; //XXX for exported FKs only?
	boolean orderByPK = true; //XXX add prop
	Boolean exportedKeys = null;
	DataDump dd = new DataDump();
	
	@Override
	public void setProperties(Properties prop) {
		super.setProperties(prop);
		startTables = Utils.getStringListFromProp(prop, PROP_SPDD_STARTTABLES, ",");
		stopTables = Utils.getStringListFromProp(prop, PROP_SPDD_STOPTABLES, ",");
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
			
			try {
				dumpTable(t, null, null, exportedKeys);
				count++;
			} catch (Exception e) {
				log.warn("error running query: "+e);
				if(failonerror) {
					throw new ProcessingException(e);
				}
			}
		}
		log.info("partitioned dump done [#start points="+count+"]");
		log.info("dumped tables ["+dumpedTablesList.size()+"]: "+dumpedTablesList);
	}

	Set<String> dumpedTables = new LinkedHashSet<String>();
	List<String> dumpedTablesList = new ArrayList<String>();
	Set<String> dumpedFKs = new LinkedHashSet<String>();
	
	void dumpTable(final Table t, final List<FK> fks, final String origFilter, final Boolean followExportedKeys) throws SQLException, IOException {
		if(!dumpedTables.add(t.getName())) {
			return;
		}
		if(stopTables.contains(t.getName())) {
			return;
		}
		
		//get filter/sql
		String filter = prop.getProperty(SPDD_PROP_PREFIX+".filter@"+t.getName());
		String join = null;
		if(filter==null && fks!=null) {
			StringBuilder sb = new StringBuilder();
			String lastTable = t.getName();
			for(int i=0;i<fks.size();i++) {
				FK fk = fks.get(i);
				if(i==0) {
					//prevents infinite recursion...
					if(!dumpedFKs.add(fk.toStringFull())) {
						return;
					}
				}
				String tableJoin = (lastTable.equals(fk.getPkTable())?fk.getFkTable():fk.getPkTable());
				sb.append("\ninner join "+tableJoin);
				lastTable = tableJoin;
				for(int ci=0;ci<fk.getPkColumns().size();ci++) {
					if(ci==0) { sb.append(" on "); }
					else { sb.append(" and "); }
					sb.append(fk.getPkTable()+"."+fk.getPkColumns().get(ci) +" = "+ fk.getFkTable()+"."+fk.getFkColumns().get(ci));
				}
			}
			join = sb.toString();
		}
		if(fks==null) {
			log.info("start table '"+t.getName()+"'"
					+(filter!=null?" [filter: "+filter+"]":""));
		}
		if(filter==null) { filter = origFilter; }
		
		//importedKeys recursive dump
		procFKs4Dump(t, fks, filter, false);
		
		//do dump
		String sql = "select distinct "+t.getName()+".* from "+t.getName()
				+(join!=null?join:"")
				+(filter!=null?"\nwhere "+filter:"");
		if(orderByPK) { 
			Constraint ctt = t.getPKConstraint();
			if(ctt!=null) {
				List<String> pkcols = new ArrayList<String>();
				for(String col: ctt.uniqueColumns) {
					//pkcols.add(quoter.get(t.getName())+"."+quoter.get(col));
					pkcols.add(qualifiedColName(t,col));
				}
				sql += "\norder by "+Utils.join(pkcols, ", ", null);
			}
		}
		log.debug("sql[followEx="+followExportedKeys+"]: "+sql);
		dd.runQuery(conn, sql, null, prop, t.getName(), t.getName());
		dumpedTablesList.add(t.getName());
		
		//exportedKeys recursive dump
		if(followExportedKeys!=null && followExportedKeys) {
			procFKs4Dump(t, fks, filter, followExportedKeys);
		}
	}
	
	void procFKs4Dump(final Table t, final List<FK> fks, final String filter, final boolean exportedKeys) throws SQLException, IOException {
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
			for(FK fk: fki) {
				String table = exportedKeys?fk.getFkTable():fk.getPkTable();
				String schema = exportedKeys?fk.getFkTableSchemaName():fk.getPkTableSchemaName();
				Table pkt = DBIdentifiable.getDBIdentifiableBySchemaAndName(model.getTables(), schema, table);
				List<FK> newFKs = new ArrayList<FK>();
				newFKs.add(fk);
				if(fks!=null) {
					newFKs.addAll(fks);
				}
				dumpTable(pkt, newFKs, filter, exportedKeys);
			}
		}
	}
	
	String qualifiedColName(Table t, String col) {
		if(quoter==null) {
			return t.getName()+"."+col;
		}
		return quoter.get(t.getName())+"."+quoter.get(col);
	}

}
