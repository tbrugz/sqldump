package tbrugz.sqldump.processors;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Properties;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import tbrugz.sqldump.dbmd.DBMSFeatures;
import tbrugz.sqldump.dbmodel.Column;
import tbrugz.sqldump.dbmodel.Table;
import tbrugz.sqldump.def.AbstractSQLProc;
import tbrugz.sqldump.def.DBMSResources;
import tbrugz.sqldump.def.Defs;
import tbrugz.sqldump.def.ProcessingException;
import tbrugz.sqldump.util.Utils;

/*
 * TODO: option to transform datatypes to ansi-like types but keep compatible with original dialect
 * ex: postgresql: int2->smallint ; text-(not!)->clob
 * ? - pgsql->ansi->pgsql ??
 */
public class SQLDialectTransformer extends AbstractSQLProc {

	public static final String PROP_PREFIX = "sqldump.schematransform";
	
	public static final String PROP_TRANSFORM_TO_ANSI = PROP_PREFIX+".toansi";
	public static final String PROP_TRANSFORM_TO_DBID = PROP_PREFIX+".todbid";
	public static final String PROP_TRANSFORM_TO_CONNID = PROP_PREFIX+".to-conn-id";
	
	static final Log log = LogFactory.getLog(SQLDialectTransformer.class);
	
	//String fromDialectId;
	boolean toANSI = false;
	boolean toConnectionDialectId = false;
	String toDialectId;
	
	@Override
	public void process() {
		if(toDialectId==null && !toConnectionDialectId && !toANSI) {
			log.warn("undefined '.todbid', '.to-conn-id' or '.toansi' property");
			if(failonerror) {
				throw new ProcessingException("SQLDialectTransformer: undefined '.todbid', '.to-conn-id' or '.toansi' property [prefix="+PROP_PREFIX+"]");
			}
			return;
		}

		if(toConnectionDialectId) {
			try {
				Connection conn = getConnection();
				DBMSFeatures feats = DBMSResources.instance().getSpecificFeatures(conn.getMetaData());
				String dialectId = feats.getId();
				log.info("using '.to-conn-id', dialect = "+dialectId);
				toDialectId = dialectId;
			}
			catch(SQLException e) {
				log.warn("error getting connection info: "+e);
				if(failonerror) {
					throw new ProcessingException(e);
				}
			}
		}

		if(!DBMSResources.instance().getDbIds().contains(toDialectId)) {
			log.warn("unknown database id: "+toDialectId);
			//toDialectId = null;
		}
		
		String fromDialectId = model.getSqlDialect();
		log.info("sql dialect transformer: from "
				+(fromDialectId==null?"ANSI-SQL (null?)":"'"+fromDialectId+"'")
				+" to "
				+(toANSI?"ANSI-SQL":"'"+toDialectId+"'")
				);
		
		int tableCount = 0, columnCount = 0;
		for(Table table: model.getTables()) {
			//log.debug("checking table '"+table.getQualifiedName()+"'");
			for(Column col: table.getColumns()) {
				String colType = col.getType();
				colType = colType.toUpperCase();
				//log.debug("checking column '"+col.getName()+"' of type '"+colType+"'");

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
				columnCount++;
			}
			tableCount++;
		}
		if(toDialectId!=null) {
			model.setSqlDialect(toDialectId);
		}
		else if(toANSI) {
			model.setSqlDialect(null); //???
		}
		log.info("model transformer ended ok [tableCount="+tableCount+"; columnCount="+columnCount+"]");
	}
	
	@SuppressWarnings("deprecation")
	@Override
	public void setProperties(Properties prop) {
		toDialectId = Utils.getPropWithDeprecated(prop, PROP_TRANSFORM_TO_DBID, Defs.PROP_TO_DB_ID, null);
		boolean transformToANSI = Utils.getPropBool(prop, PROP_TRANSFORM_TO_ANSI);
		boolean transformToConnId = Utils.getPropBool(prop, PROP_TRANSFORM_TO_CONNID);

		if(toDialectId==null && transformToConnId) {
			toConnectionDialectId = true;
		}

		if(toDialectId==null && transformToANSI) {
			toANSI = true;
		}

		/*if(toDialectId==null) {
			log.warn("target database id undefined");
		}
		else if(!DBMSResources.instance().getDbIds().contains(toDialectId)) {
			log.warn("unknown database id: "+toDialectId);
			toDialectId = null;
		}*/
	}

	@Override
	public boolean needsConnection() {
		return toConnectionDialectId;
	}

}
