package tbrugz.sqldump.processors;

import java.util.Properties;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import tbrugz.sqldump.dbmodel.Column;
import tbrugz.sqldump.dbmodel.Table;
import tbrugz.sqldump.def.AbstractSQLProc;
import tbrugz.sqldump.def.DBMSResources;
import tbrugz.sqldump.def.Defs;
import tbrugz.sqldump.def.ProcessingException;

public class SQLDialectTransformer extends AbstractSQLProc {

	static Log log = LogFactory.getLog(SQLDialectTransformer.class);
	
	//String fromDialectId;
	String toDialectId;
	
	@Override
	public void process() {
		if(toDialectId==null) {
			log.warn("undefined toDialectId");
			if(failonerror) {
				throw new ProcessingException("SQLDialectTransformer: undefined toDialectId");
			}
			return;
		}
		
		String fromDialectId = model.getSqlDialect();
		log.info("sql dialect transformer: from '"+fromDialectId+"' to '"+toDialectId+"'");
		
		for(Table table: model.getTables()) {
			for(Column col: table.getColumns()) {
				String colType = col.type;
				
				colType = colType.toUpperCase();
				//String ansiColType = ColTypeUtil.dbmsSpecificProps.getProperty("from."+fromDialectId+"."+colType);
				String ansiColType = DBMSResources.instance().toANSIType(fromDialectId, colType);
				String newColType = null;
				if(ansiColType!=null) {
					ansiColType = ansiColType.toUpperCase();
					newColType = DBMSResources.instance().toSQLDialectType(toDialectId, ansiColType);
					//newColType = ColTypeUtil.dbmsSpecificProps.getProperty("to."+toDialectId+"."+ansiColType);
				}
				
				if(newColType!=null) {
					col.type = newColType;
					 
				}
				else if(ansiColType!=null) {
					col.type = ansiColType;
				}
				/*else {
					//log.debug("old col type: "+colType);
					colType = col.type.trim();
				}*/
				
			}
		}
		log.info("model transformer ended ok");
	}
	
	@Override
	public void setProperties(Properties prop) {
		toDialectId = prop.getProperty(Defs.PROP_TO_DB_ID);
		if(!DBMSResources.instance().getDbIds().contains(toDialectId)) {
			log.warn("unknown database id: "+toDialectId);
			toDialectId = null;
		}
	}

}
