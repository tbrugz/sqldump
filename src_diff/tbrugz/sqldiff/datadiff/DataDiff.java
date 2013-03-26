package tbrugz.sqldiff.datadiff;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import tbrugz.sqldiff.SQLDiff;
import tbrugz.sqldump.datadump.DataDump;
import tbrugz.sqldump.dbmodel.Constraint;
import tbrugz.sqldump.dbmodel.SchemaModel;
import tbrugz.sqldump.dbmodel.Table;
import tbrugz.sqldump.def.DBMSResources;
import tbrugz.sqldump.util.StringDecorator;
import tbrugz.sqldump.util.Utils;

public class DataDiff {

	static Log log = LogFactory.getLog(DataDiff.class);
	
	public static final String PROP_DATADIFF_TABLES = SQLDiff.PROP_PREFIX+".datadiff.tables";
	
	StringDecorator quoteAllDecorator;
	
	Properties prop = null;
	List<String> tablesToDiffFilter = new ArrayList<String>();
	SchemaModel sourceSchemaModel = null;
	SchemaModel targetSchemaModel = null;
	Connection sourceConn = null;
	Connection targetConn = null;
	
	public void setProperties(Properties prop) {
		tablesToDiffFilter = Utils.getStringListFromProp(prop, PROP_DATADIFF_TABLES, ",");
		String quote = DBMSResources.instance().getIdentifierQuoteString();
		quoteAllDecorator = new StringDecorator.StringQuoterDecorator(quote);
		
		this.prop = prop;
	}

	public void setFailOnError(boolean failonerror) {
	}
	
	public void setSourceSchemaModel(SchemaModel schemamodel) {
		sourceSchemaModel = schemamodel;
	}

	public void setTargetSchemaModel(SchemaModel schemamodel) {
		targetSchemaModel = schemamodel;
	}

	/*
	public void setSchemaDiff(SchemaDiff schemadiff) {
	}

	public void setSourceGrabber(SchemaModelGrabber grabber) {
	}

	public void setTargetGrabber(SchemaModelGrabber grabber) {
	}
	*/

	public void setSourceConnection(Connection conn) {
		sourceConn = conn;
	}

	public void setTargetConnection(Connection conn) {
		targetConn = conn;
	}

	public void process() throws SQLException {
		Set<Table> tablesToDiff = sourceSchemaModel.getTables();
		Set<Table> targetTables = targetSchemaModel.getTables();
		tablesToDiff.retainAll(targetTables);
		
		//XXX: test if source & target conn are valid; if not, create based on properties
		
		ResultSetDiff rsdiff = new ResultSetDiff();
		
		for(Table table: tablesToDiff) {
			if(tablesToDiffFilter!=null) {
				if(tablesToDiffFilter.contains(table.getName())) {
					tablesToDiffFilter.remove(table.getName());
				}
				else {
					continue;
				}
			}
			String sql = DataDump.getQuery(table, "*", null, null, true);
			
			Statement stmtSource = sourceConn.createStatement();
			ResultSet rsSource = stmtSource.executeQuery(sql);
			
			Statement stmtTarget = targetConn.createStatement();
			ResultSet rsTarget = stmtTarget.executeQuery(sql);
			
			//XXX: check if rsmetadata is equal between RSs...
			
			List<String> keyCols = null;
			Constraint ctt = table.getPKConstraint();
			if(ctt!=null) {
				keyCols = ctt.uniqueColumns;
			}
			if(keyCols==null) {
				log.warn("table '"+table+"' has no PK. Diff disabled");
				continue;
			}
			
			log.info("diff for table '"+table+"'...");
			rsdiff.diff(rsSource, rsTarget, keyCols);
			
			rsSource.close(); rsTarget.close();
		}

		if(tablesToDiffFilter!=null && tablesToDiffFilter.size()>0) {
			log.warn("tables not found for diff: "+Utils.join(tablesToDiffFilter, ", "));
		}
	}
	
}