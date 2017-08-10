package tbrugz.sqldump.processors;

import java.util.Properties;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import tbrugz.sqldump.dbmodel.Column;
import tbrugz.sqldump.dbmodel.Table;
import tbrugz.sqldump.def.AbstractSchemaProcessor;
import tbrugz.sqldump.def.DBMSResources;
import tbrugz.sqldump.def.Defs;
import tbrugz.sqldump.def.ProcessingException;
import tbrugz.sqldump.util.Utils;

/*
 * TODO: option to transform datatypes to ansi-like types but keep compatible with original dialect
 * ex: postgresql: int2->smallint ; text-(not!)->clob
 * ? - pgsql->ansi->pgsql ??
 */
public class SQLDialectTransformer extends AbstractSchemaProcessor {

	public static final String PROP_PREFIX = "sqldump.schematransform";
	
	public static final String PROP_TRANSFORM_TO_ANSI = PROP_PREFIX+".toansi";
	public static final String PROP_TRANSFORM_TO_DBID = PROP_PREFIX+".todbid";
	
	static final Log log = LogFactory.getLog(SQLDialectTransformer.class);
	
	//String fromDialectId;
	boolean toANSI = false;
	String toDialectId;
	
	@Override
	public void process() {
		if(toDialectId==null && !toANSI) {
			log.warn("undefined toDialectId or '.toansi' property");
			if(failonerror) {
				throw new ProcessingException("SQLDialectTransformer: undefined toDialectId or '.toansi' property [prefix="+PROP_PREFIX+"]");
			}
			return;
		}
		
		String fromDialectId = model.getSqlDialect();
		log.info("sql dialect transformer: from '"+fromDialectId+"' to "
				+(toANSI?"ANSI-SQL":"'"+toDialectId+"'")
				);
		
		for(Table table: model.getTables()) {
			for(Column col: table.getColumns()) {
				String colType = col.getType();
				
				colType = colType.toUpperCase();
				//String ansiColType = ColTypeUtil.dbmsSpecificProps.getProperty("from."+fromDialectId+"."+colType);
				String ansiColType = DBMSResources.instance().toANSIType(fromDialectId, colType);
				String newColType = null;
				if(ansiColType!=null) {
					ansiColType = ansiColType.toUpperCase();
					newColType = DBMSResources.instance().toSQLDialectType(toDialectId, ansiColType);
					//log.debug("orig type '"+colType+"', ansi type '"+ansiColType+"', new col type '"+newColType+"'");
					//newColType = ColTypeUtil.dbmsSpecificProps.getProperty("to."+toDialectId+"."+ansiColType);
				}
				else {
					// ansi type is null, use original type
					newColType = DBMSResources.instance().toSQLDialectType(toDialectId, colType.toUpperCase());
					//log.debug("orig type '"+colType+"', ansi type '"+ansiColType+"', new col type '"+newColType+"'");
				}
				
				if(newColType!=null && !newColType.equalsIgnoreCase(colType)) {
					col.setType(newColType);
					log.debug("["+table.getName()+"] orig type '"+colType+"', ansi type '"+ansiColType+"', new col type '"+newColType+"'");
				}
				else if(ansiColType!=null) {
					col.setType(ansiColType);
					log.debug("["+table.getName()+"] orig type '"+colType+"', ansi type '"+ansiColType+"', new col type '"+newColType+"'");
				}
				/*else {
					//log.debug("old col type: "+colType);
					colType = col.type.trim();
				}*/
				
			}
		}
		if(toDialectId!=null) {
			model.setSqlDialect(toDialectId);
		}
		else if(toANSI) {
			model.setSqlDialect(null); //???
		}
		log.info("model transformer ended ok");
	}
	
	@SuppressWarnings("deprecation")
	@Override
	public void setProperties(Properties prop) {
		toDialectId = Utils.getPropWithDeprecated(prop, PROP_TRANSFORM_TO_DBID, Defs.PROP_TO_DB_ID, null);
		if(toDialectId==null) {
			Boolean transformToANSI = Utils.getPropBoolean(prop, PROP_TRANSFORM_TO_ANSI);
			if(transformToANSI!=null) {
				toANSI = transformToANSI;
			}
			else {
				log.warn("transform to database-id/ansi undefined");
			}
		}
		else if(!DBMSResources.instance().getDbIds().contains(toDialectId)) {
			log.warn("unknown database id: "+toDialectId);
			toDialectId = null;
		}
	}

}
