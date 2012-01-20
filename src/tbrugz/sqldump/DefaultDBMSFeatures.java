package tbrugz.sqldump;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Properties;

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
}
