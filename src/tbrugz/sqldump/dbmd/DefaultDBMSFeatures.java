package tbrugz.sqldump.dbmd;

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
	
	@Override
	public String sqlDefaultDateFormatPattern() {
		return null;
	}

	@Override
	public void grabDBViews(SchemaModel model, String schemaPattern, String viewNamePattern, Connection conn) throws SQLException {
	}

	@Override
	public void grabDBTriggers(SchemaModel model, String schemaPattern, String tableNamePattern, String triggerNamePattern, Connection conn) throws SQLException {
	}

	@Override
	public void grabDBExecutables(SchemaModel model, String schemaPattern, String execNamePattern, Connection conn) throws SQLException {
	}

	@Override
	public void grabDBSequences(SchemaModel model, String schemaPattern, String sequenceNamePattern, Connection conn) throws SQLException {
	}

	@Override
	public void grabDBSynonyms(SchemaModel model, String schemaPattern, String synonymNamePattern, Connection conn) throws SQLException {
	}

	@Override
	public void grabDBCheckConstraints(SchemaModel model, String schemaPattern, String constraintNamePattern, Connection conn) throws SQLException {
	}

	@Override
	public void grabDBUniqueConstraints(SchemaModel model, String schemaPattern, String constraintNamePattern, Connection conn) throws SQLException {
	}

}
