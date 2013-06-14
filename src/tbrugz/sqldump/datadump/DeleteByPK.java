package tbrugz.sqldump.datadump;

import java.io.IOException;
import java.io.Writer;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import tbrugz.sqldump.util.SQLUtils;
import tbrugz.sqldump.util.Utils;

public class DeleteByPK extends UpdateByPKDataDump {
	static final String DELETEBYPK_SYNTAX_ID = "deletebypk";
	static final String DELETEBYPK_EXT = "dbpk.sql";
	
	static Log log = LogFactory.getLog(DeleteByPK.class);
	
	@Override
	public String getSyntaxId() {
		return DELETEBYPK_SYNTAX_ID;
	}
	
	@Override
	public String getDefaultFileExtension() {
		return DELETEBYPK_EXT;
	}
	
	@Override
	public void dumpRow(ResultSet rs, long count, Writer fos) throws IOException, SQLException {
		if(pkCols==null) { return; }
		
		//XXX: only get PK/UK values?
		List<String> vals = (List<String>) DataDumpUtils.values4sql( SQLUtils.getRowObjectListFromRS(rs, lsColTypes, numCol), dateFormatter );
		List<String> wheres = new ArrayList<String>();

		for(int i = 0;i<lsColNames.size();i++) {
			String colname = lsColNames.get(i);
			if(pkCols.contains(colname)) {
				wheres.add(colname+" = "+vals.get(i));
			}
		}
		
		out("delete from "+tableName+
			" where "+
			Utils.join(wheres, " and ")+
			";", fos);
	}
	
}
