package tbrugz.sqldump.datadump;

import java.io.Writer;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.Logger;

import tbrugz.sqldump.SQLUtils;
import tbrugz.sqldump.Utils;

public class UpdateByPKDataDump extends InsertIntoDataDump {
	static final String UPDATEBYPK_SYNTAX_ID = "updatebypk";
	static final String UPDATEBYPK_EXT = "ubpk.sql";
	
	static Logger log = Logger.getLogger(UpdateByPKDataDump.class);
	
	List<String> pkCols = null; 
	
	@Override
	public String getSyntaxId() {
		return UPDATEBYPK_SYNTAX_ID;
	}
	
	@Override
	public String getDefaultFileExtension() {
		return UPDATEBYPK_EXT;
	}
	
	@Override
	public void initDump(String tableName, List<String> cols, ResultSetMetaData md)
			throws SQLException {
		pkCols = cols;
		if(cols!=null && pkCols.size()==0) {
			pkCols = null;
		}
		if(pkCols==null) {
			log.warn("can't dump: needs unique key [query/table: "+tableName+"]");
			return;
		}
		super.initDump(tableName, null, md);
	}
	
	@Override
	public void dumpRow(ResultSet rs, long count, Writer fos) throws Exception {
		if(pkCols==null) { return; }
		
		List<String> vals = (List<String>) Utils.values4sql( SQLUtils.getRowObjectListFromRS(rs, lsColTypes, numCol), dateFormatter );
		//StringBuffer sb = new StringBuffer();
		//sb.append("update "+tableName+" set ");
		List<String> sets = new ArrayList<String>();
		List<String> wheres = new ArrayList<String>();

		//StringBuffer sbWhere = new StringBuffer();
		//boolean isFirst = true;
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
