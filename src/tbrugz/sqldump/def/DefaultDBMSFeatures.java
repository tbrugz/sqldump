package tbrugz.sqldump.def;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;
import java.util.Properties;

import tbrugz.sqldump.dbmodel.SchemaModel;
import tbrugz.sqldump.dbmodel.Table;
import tbrugz.sqldump.dbmodel.View;

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
	public List<View> grabDBViews(SchemaModel model, String schemaPattern, String tablePattern,
			Connection conn) throws SQLException {
		return null;
	}

	@Override
	public Table getTableObject() {
		return new Table();
	}
}
