package tbrugz.sqldump.dbmsfeatures;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import tbrugz.sqldump.dbmd.DefaultDBMSFeatures;
import tbrugz.sqldump.dbmodel.Column;
import tbrugz.sqldump.dbmodel.DBObject;
import tbrugz.sqldump.dbmodel.NamedDBObject;

public class ApacheHiveFeatures extends DefaultDBMSFeatures {

	static final Log log = LogFactory.getLog(ApacheHiveFeatures.class);
	
	@Override
	public String sqlAlterColumnClause() {
		return "change column";
	}
	
	@Override
	public String ddlAlterColumn(NamedDBObject table, Column column, String xtraSql) {
		return "alter table "+DBObject.getFinalName(table, true)+" "+sqlAlterColumnClause()
			+" "+column.getName()+" "+column.getName()
			+(xtraSql!=null?xtraSql:"");
		//return super.ddlAlterColumn(table, column, xtraSql);
	}

}
