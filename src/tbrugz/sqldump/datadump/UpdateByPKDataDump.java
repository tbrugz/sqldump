package tbrugz.sqldump.datadump;

import java.io.IOException;
import java.io.Writer;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import tbrugz.sqldump.util.SQLUtils;
import tbrugz.sqldump.util.Utils;

/*
 * XXX: do not create output file if pkCols==null
 */
public class UpdateByPKDataDump extends InsertIntoDataDump {
	static final String UPDATEBYPK_SYNTAX_ID = "updatebypk";
	static final String UPDATEBYPK_EXT = "ubpk.sql";
	
	static final Log log = LogFactory.getLog(UpdateByPKDataDump.class);
	
	@Override
	public String getSyntaxId() {
		return UPDATEBYPK_SYNTAX_ID;
	}
	
	@Override
	public String getDefaultFileExtension() {
		return UPDATEBYPK_EXT;
	}
	
	@Override
	public void initDump(String schemaName, String tableName, List<String> cols, ResultSetMetaData md)
			throws SQLException {
		pkCols = cols;
		if(cols!=null && pkCols.size()==0) {
			pkCols = null;
		}
		if(pkCols==null) {
			log.warn("can't dump: needs unique key [query/table: "+tableName+"]");
			//XXX: throw RuntimeException / IllegalArgument ?
			return;
		}
		super.initDump(schemaName, tableName, cols, md);
	}
	
	@Override
	public void dumpRow(ResultSet rs, long count, Writer fos) throws IOException, SQLException {
		if(pkCols==null) { return; }
		
		List<String> vals = (List<String>) DataDumpUtils.values4sql( SQLUtils.getRowObjectListFromRS(rs, lsColTypes, numCol), dateFormatter );
		List<String> sets = new ArrayList<String>();
		List<String> wheres = new ArrayList<String>();

		for(int i = 0;i<lsColNames.size();i++) {
			String colname = lsColNames.get(i);
			if(!pkCols.contains(colname)) {
				sets.add(colname+" = "+vals.get(i));
			}
			else {
				wheres.add(colname+" = "+vals.get(i));
			}
		}
		
		out("update "+tableName+" set "+
			Utils.join(sets, ", ")+
			" where "+
			Utils.join(wheres, " and ")+
			";", fos);
	}
	
}
