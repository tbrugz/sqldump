package tbrugz.sqldump.dbmd;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Collection;
import java.util.List;

import tbrugz.sqldump.dbmodel.DBObjectType;
import tbrugz.sqldump.dbmodel.ExecutableObject;
import tbrugz.sqldump.dbmodel.SchemaModel;
import tbrugz.sqldump.dbmodel.Sequence;
import tbrugz.sqldump.dbmodel.Synonym;
import tbrugz.sqldump.dbmodel.Table;
import tbrugz.sqldump.dbmodel.Trigger;
import tbrugz.sqldump.dbmodel.View;

public class DefaultDBMSFeatures extends AbstractDBMSFeatures {

	@Override
	public void grabDBObjects(SchemaModel model, String schemaPattern,
			Connection conn) throws SQLException {
	}

	/*@Override
	public void grabDBViews(SchemaModel model, String schemaPattern, String viewNamePattern, Connection conn) throws SQLException {
		grabDBViews(model.getViews(), schemaPattern, viewNamePattern, conn);
	}

	@Override
	public void grabDBTriggers(SchemaModel model, String schemaPattern, String tableNamePattern, String triggerNamePattern, Connection conn) throws SQLException {
		grabDBTriggers(model.getTriggers(), schemaPattern, tableNamePattern, triggerNamePattern, conn);
	}

	@Override
	public void grabDBExecutables(SchemaModel model, String schemaPattern, String execNamePattern, Connection conn) throws SQLException {
		grabDBExecutables(model.getExecutables(), schemaPattern, execNamePattern, conn);
	}

	@Override
	public void grabDBSequences(SchemaModel model, String schemaPattern, String sequenceNamePattern, Connection conn) throws SQLException {
		grabDBSequences(model.getSequences(), schemaPattern, sequenceNamePattern, conn);
	}

	@Override
	public void grabDBSynonyms(SchemaModel model, String schemaPattern, String synonymNamePattern, Connection conn) throws SQLException {
		grabDBSynonyms(model.getSynonyms(), schemaPattern, synonymNamePattern, conn);
	}

	@Override
	public void grabDBCheckConstraints(SchemaModel model, String schemaPattern, String constraintNamePattern, Connection conn) throws SQLException {
		grabDBCheckConstraints(model.getTables(), schemaPattern, constraintNamePattern, conn);
	}

	@Override
	public void grabDBUniqueConstraints(SchemaModel model, String schemaPattern, String constraintNamePattern, Connection conn) throws SQLException {
		grabDBUniqueConstraints(model.getTables(), schemaPattern, constraintNamePattern, conn);
	}*/

	@Override
	public void grabDBViews(Collection<View> views, String schemaPattern,
			String viewNamePattern, Connection conn) throws SQLException {
	}

	@Override
	public void grabDBTriggers(Collection<Trigger> triggers, String schemaPattern,
			String tableNamePattern, String triggerNamePattern, Connection conn)
			throws SQLException {
	}

	@Override
	public void grabDBExecutables(Collection<ExecutableObject> execs,
			String schemaPattern, String execNamePattern, Connection conn)
			throws SQLException {
	}

	@Override
	public void grabDBSequences(Collection<Sequence> seqs, String schemaPattern,
			String sequenceNamePattern, Connection conn) throws SQLException {
	}

	@Override
	public void grabDBSynonyms(Collection<Synonym> synonyms, String schemaPattern,
			String synonymNamePattern, Connection conn) throws SQLException {
	}

	@Override
	public void grabDBCheckConstraints(Collection<Table> tables,
			String schemaPattern, String constraintNamePattern, Connection conn)
			throws SQLException {
	}

	@Override
	public void grabDBUniqueConstraints(Collection<Table> tables,
			String schemaPattern, String constraintNamePattern, Connection conn)
			throws SQLException {
	}

	@Override
	public List<DBObjectType> getExecutableObjectTypes() {
		return null;
	}

	/*
	@Override
	public boolean supportsGrabViews() {
		return false;
	}

	@Override
	public boolean supportsGrabTriggers() {
		return false;
	}

	@Override
	public boolean supportsGrabExecutables() {
		return false;
	}

	@Override
	public boolean supportsGrabSequences() {
		return false;
	}

	@Override
	public boolean supportsGrabSynonyms() {
		return false;
	}

	@Override
	public boolean supportsGrabCheckConstraints() {
		return false;
	}

	@Override
	public boolean supportsGrabUniqueConstraints() {
		return false;
	}
	*/
	
	@Override
	public boolean supportsExplainPlan() {
		return false;
	}
	
	@Override
	public ResultSet explainPlan(String sql, Connection conn) throws SQLException {
		throw new UnsupportedOperationException("explainPlan not implemented");
	}

}
