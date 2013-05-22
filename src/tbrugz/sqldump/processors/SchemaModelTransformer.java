package tbrugz.sqldump.processors;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import tbrugz.sqldump.dbmodel.Column;
import tbrugz.sqldump.dbmodel.DBIdentifiable;
import tbrugz.sqldump.dbmodel.FK;
import tbrugz.sqldump.dbmodel.SchemaModel;
import tbrugz.sqldump.dbmodel.Table;
import tbrugz.sqldump.def.AbstractSQLProc;
import tbrugz.sqldump.util.Utils;
import tbrugz.util.LongFactory;
import tbrugz.util.NonNullGetMap;

public class SchemaModelTransformer extends AbstractSQLProc {
	static final Log log = LogFactory.getLog(SchemaModelTransformer.class);
	
	static final String DEFAULT_PREFIX = "sqldump.modeltransform";
	
	static final String SUFFIX_REMOVE_SCHEMANAME = ".removeschemaname";
	static final String SUFFIX_REMOVE_FKS_BYNAME = ".removefksbyname";

	SchemaModel schemaModel;
	List<FK> addedFKs;
	String prefix = DEFAULT_PREFIX;
	
	boolean doRemoveSchemaName = false;
	
	@Override
	public void setSchemaModel(SchemaModel schemamodel) {
		super.setSchemaModel(schemamodel);
		this.schemaModel = schemamodel; 
	}
	
	@Override
	public void setProperties(Properties prop) {
		this.prop = prop;
		doRemoveSchemaName = Utils.getPropBool(prop, prefix+SUFFIX_REMOVE_SCHEMANAME, doRemoveSchemaName);
	}

	@Override
	public void process() {
		removeFKs();
		addFKs();
		
		alterColymnType();
		
		if(doRemoveSchemaName) {
			removeSchemaname();
		}
	}

	//moved from SchemaModelTransformer.dumpSchema()
	void addFKs() {
		addedFKs = new ArrayList<FK>();
		for(Table t: schemaModel.getTables()) {
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
		schemaModel.getForeignKeys().addAll(addedFKs);
	}

	void removeFKs() {
		int count = 0;
		List<String> fksToRemove = Utils.getStringListFromProp(prop, prefix+SUFFIX_REMOVE_FKS_BYNAME, ",");
		Iterator<FK> i = schemaModel.getForeignKeys().iterator();
		if(fksToRemove!=null) {
			while (i.hasNext()) {
				FK fk = i.next();
				//boolean removeFK = Utils.getPropBool(prop, prefix+".fk@"+fk.getName()+".remove", false);
				//if(removeFK) {
				if(fksToRemove.contains(fk.getName())) {
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
		for(Table t: schemaModel.getTables()) {
			for(Column c: t.getColumns()) {
				String newColType = prop.getProperty(prefix+".columntype@"+c.type);
				if(newColType!=null) {
					colTypeTransforms.put(c.type, colTypeTransforms.get(c.type)+1);
					//log.info("prop: "+prefix+".columntype@"+c.type+" newValue="+newColType);
					c.type = newColType;
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
		removeSchemaname(schemaModel.getTables());
		removeSchemaname(schemaModel.getForeignKeys());
		removeSchemaname(schemaModel.getIndexes());
		removeSchemaname(schemaModel.getExecutables());
		removeSchemaname(schemaModel.getSequences());
		removeSchemaname(schemaModel.getSynonyms());
		removeSchemaname(schemaModel.getTriggers());
		removeSchemaname(schemaModel.getViews());
	}

	void removeSchemaname(Set<? extends DBIdentifiable> list) {
		for(DBIdentifiable o: list) {
			o.setSchemaName(null);
		}
	}
}
