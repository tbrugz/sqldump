package tbrugz.sqldump.def;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Properties;

import tbrugz.sqldump.dbmodel.FK;
import tbrugz.sqldump.dbmodel.SchemaModel;
import tbrugz.sqldump.dbmodel.Table;

public class DefaultDBMSFeatures extends AbstractDBMSFeatures {

	@Override
	public void procProperties(Properties prop) {
		super.procProperties(prop);
	}

	@Override
	public void grabDBObjects(SchemaModel model, String schemaPattern,
			Connection conn) throws SQLException {
	}

	@Override
	public Table getTableObject() {
		return new Table();
	}

	@Override
	public FK getForeignKeyObject() {
		return new FK();
	}
}
