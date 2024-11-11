package tbrugz.sqldump.processors;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.TreeSet;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import tbrugz.sqldump.dbmodel.Column;
import tbrugz.sqldump.dbmodel.DBIdentifiable;
import tbrugz.sqldump.dbmodel.FK;
import tbrugz.sqldump.dbmodel.Query;
import tbrugz.sqldump.dbmodel.Relation;
import tbrugz.sqldump.dbmodel.Table;
import tbrugz.sqldump.dbmodel.View;
import tbrugz.sqldump.def.AbstractSchemaProcessor;
import tbrugz.sqldump.util.Utils;
import tbrugz.util.LongFactory;
import tbrugz.util.NonNullGetMap;

public class SchemaModelTransformer extends AbstractSchemaProcessor {
	static final Log log = LogFactory.getLog(SchemaModelTransformer.class);
	
	static final String DEFAULT_PREFIX = "sqldump.modeltransform";
	
	static final String SUFFIX_REMOVE_SCHEMANAME = ".removeschemaname";
	static final String SUFFIX_REMOVE_FKS_BYNAME = ".removefksbyname";
	static final String SUFFIX_REMOVE_TABLES_WITH_FKS = ".removetableswithfks"; //remove tables with *their* fks
	static final String SUFFIX_REMOVE_VIEWS_DEFINITIONS = ".remove-views-definitions";
	
	//XXX option to remove FKs that references non-existent tables?
	//XXX option to remove FKs from/to table?

	List<FK> addedFKs;
	String prefix = DEFAULT_PREFIX;
	
	boolean doRemoveSchemaName = false;
	boolean doRemoveViewsDefinitions = false;
	
	@Override
	public void setProperties(Properties prop) {
		super.setProperties(prop);
		doRemoveSchemaName = Utils.getPropBool(prop, prefix+SUFFIX_REMOVE_SCHEMANAME, doRemoveSchemaName);
		doRemoveViewsDefinitions = Utils.getPropBool(prop, prefix+SUFFIX_REMOVE_VIEWS_DEFINITIONS, doRemoveSchemaName);
	}

	@Override
	public void process() {
		//tables
		List<String> tablesToRemove = Utils.getStringListFromProp(prop, prefix+SUFFIX_REMOVE_TABLES_WITH_FKS, ",");
		if(tablesToRemove!=null) {
			removeTablesWithFKs(tablesToRemove);
		}
		
		//views
		if(doRemoveViewsDefinitions) {
			removeViewsDefinitions();
		}
		
		//fks
		removeFKs();
		addFKs();
		
		//columns
		alterColymnType();
		
		//other metadata
		if(doRemoveSchemaName) {
			removeSchemaname();
		}
		
		log.info("model transformer ended ok");
	}

	//moved from SchemaModelTransformer.dumpSchema()
	void addFKs() {
		addedFKs = new ArrayList<FK>();
		Set<Relation> relations = new TreeSet<Relation>();
		relations.addAll(model.getTables());
		relations.addAll(model.getViews());
		for(Relation t: relations) {
			//sqldump.modeltransform.table@<fktable>.xtrafk=<fkcolumn>:[<schema>.]<pktable>:<pkcolumn>[:<fk-name>][;(...)]
			List<String> xtraFKs = Utils.getStringListFromProp(prop, prefix+".table@"+t.getName()+".xtrafk", ";");
			if(xtraFKs==null) { continue; }
			
			for(String newfkstr: xtraFKs) {
				String[] parts = newfkstr.split(":");
				if(parts.length<3) {
					log.warn("wrong number of FK parts: "+parts.length+"; should be 3 or 4 [fkstr='"+newfkstr+"']");
					continue;
				}
				FK fk = new FK();
				log.debug("new FK: "+newfkstr+"; parts.len: "+parts.length);
				
				fk.setFkTable(t.getName());
				fk.setFkTableSchemaName(t.getSchemaName());
				
				List<String> fkcols = Utils.newStringList(parts[0].split(","));
				fk.setFkColumns(fkcols);
				
				if(parts[1].contains(".")) {
					String[] pkTableParts = parts[1].split("\\.");
					log.debug("FKschema: "+parts[1]+"; pkTable.len: "+pkTableParts.length);
					fk.setPkTableSchemaName(pkTableParts[0]);
					parts[1] = pkTableParts[1];
				}
				fk.setPkTable( parts[1] );
				
				List<String> pkcols = Utils.newStringList(parts[2].split(","));
				fk.setPkColumns( pkcols );
				if(parts.length>3) {
					fk.setName(parts[3]);
				}
				else {
					fk.setName(fk.getFkTable() + "_" + fk.getPkTable() + "_FK"); //XXX: suggestAcronym?
				}

				if(fkcols.size()!=pkcols.size()) {
					log.warn("number of FK columns ["+fkcols.size()+"] != number of PK columns ["+pkcols.size()+"] ; FK: "+fk.toStringFull());
					continue;
				}
				//XXX: changing model... should clone() first?
				if(addedFKs.add(fk)) {
					log.info("FK added: "+fk);
				}
			}
		}
		model.getForeignKeys().addAll(addedFKs);
	}

	void removeFKs() {
		int count = 0;
		List<String> fksToRemove = Utils.getStringListFromProp(prop, prefix+SUFFIX_REMOVE_FKS_BYNAME, ",");
		Iterator<FK> i = model.getForeignKeys().iterator();
		if(fksToRemove!=null) {
			while (i.hasNext()) {
				FK fk = i.next();
				//boolean removeFK = Utils.getPropBool(prop, prefix+".fk@"+fk.getName()+".remove", false);
				//if(removeFK) {
				if(fksToRemove.contains(fk.getName())) {
					log.debug("FK removed: "+fk);
					i.remove();
					count++;
				}
			}
		}
		if(count>0) {
			log.info(count+" FKs removed");
		}
	}
	
	Map<String, Long> colTypeTransforms = new NonNullGetMap<String, Long>(new HashMap<String, Long>(), new LongFactory());

	void alterColymnType() {
		//sqldump.schematransform.columntype@DATE=TIMESTAMP
		int count = 0;
		for(Table t: model.getTables()) {
			if(t.getColumns()==null) { continue; }
			for(Column c: t.getColumns()) {
				String newColType = prop.getProperty(prefix+".columntype@"+c.getType());
				if(newColType!=null) {
					colTypeTransforms.put(c.getType(), colTypeTransforms.get(c.getType())+1);
					//log.info("prop: "+prefix+".columntype@"+c.type+" newValue="+newColType);
					c.setType(newColType);
					count++;
				}
			}
		}
		if(count>0) {
			log.info(count+" column types changed ["+Utils.countByKeyString(colTypeTransforms)+"]");
		}
	} 
	
	/*public void removeAllFKs() {
		schemaModel.getForeignKeys().removeAll(addedFKs);
	}*/

	public List<FK> getAddedFKs() {
		return addedFKs;
	}
	
	void removeSchemaname() {
		removeSchemaname(model.getTables());
		removeSchemaname(model.getForeignKeys());
		removeSchemaname(model.getIndexes());
		removeSchemaname(model.getExecutables());
		removeSchemaname(model.getSequences());
		removeSchemaname(model.getSynonyms());
		removeSchemaname(model.getTriggers());
		removeSchemaname(model.getViews());
	}

	void removeSchemaname(Set<? extends DBIdentifiable> list) {
		for(DBIdentifiable o: list) {
			o.setSchemaName(null);
		}
	}
	
	void removeTablesWithFKs(List<String> removeTables) {
		int tablecount = 0, fkcount = 0;
		Iterator<Table> ti = model.getTables().iterator();
		while (ti.hasNext()) {
			Table t = ti.next();
			if(removeTables.contains(t.getName())) {
				Iterator<FK> fi = model.getForeignKeys().iterator();
				while (fi.hasNext()) {
					FK fk = fi.next();
					if(fk.getPkTable().equals(t.getName()) || fk.getFkTable().equals(t.getName())) { //XXX: fkTable-only or pkTable-only?
						fi.remove();
						fkcount++;
						log.debug("fk removed: "+fk);
					}
				}
				ti.remove();
				tablecount++;
				removeTables.remove(t.getName());
				log.debug("table removed: "+t);
			}
		}
		if(tablecount>0) {
			log.info("removed "+tablecount+" tables with "+fkcount+" FKs");
		}
		if(removeTables.size()>0) {
			log.info("remove: tables not found: "+removeTables);
		}
	}
	
	void removeViewsDefinitions() {
		Set<View> views = model.getViews();
		int count = 0;
		for(View v: views) {
			v.setQuery(null);
			//XXX prop to remove parameterCount, parameterValues?
			if(v instanceof Query) {
				Query q = (Query) v;
				q.setParameterCount(null);
				q.setParameterValues(null);
			}
			count++;
		}
		if(count>0) {
			log.info(count+" views' definitions removed");
		}
	}
}
