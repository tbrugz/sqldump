package tbrugz.sqldump;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Properties;

import tbrugz.sqldump.dbmodel.Table;

public class DefaultDBMSFeatures extends AbstractDBMSFeatures {

	public void procProperties(Properties prop) {
	}

	public void grabDBObjects(SchemaModel model, String schemaPattern,
			Connection conn) throws SQLException {
	}

	public Table getTableObject() {
		return new Table();
	}
}
