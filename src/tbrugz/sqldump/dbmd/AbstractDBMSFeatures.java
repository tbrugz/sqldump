package tbrugz.sqldump.dbmd;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import tbrugz.sqldump.dbmodel.Column;
import tbrugz.sqldump.dbmodel.DBObject;
import tbrugz.sqldump.dbmodel.ExecutableObject;
import tbrugz.sqldump.dbmodel.FK;
import tbrugz.sqldump.dbmodel.NamedDBObject;
import tbrugz.sqldump.dbmodel.Table;
import tbrugz.sqldump.util.Utils;

public abstract class AbstractDBMSFeatures implements DBMSFeatures {

	public static final String PROP_GRAB_INDEXES = "sqldump.dbspecificfeatures.grabindexes";
	public static final String PROP_GRAB_EXECUTABLES = "sqldump.dbspecificfeatures.grabexecutables";
	public static final String PROP_GRAB_VIEWS = "sqldump.dbspecificfeatures.grabviews";
	public static final String PROP_GRAB_MATERIALIZED_VIEWS = "sqldump.dbspecificfeatures.grabmaterializedviews";
	public static final String PROP_GRAB_TRIGGERS = "sqldump.dbspecificfeatures.grabtriggers";
	public static final String PROP_GRAB_SYNONYMS = "sqldump.dbspecificfeatures.grabsynonyms";
	public static final String PROP_GRAB_SEQUENCES = "sqldump.dbspecificfeatures.grabsequences";
	public static final String PROP_GRAB_CONSTRAINTS_XTRA = "sqldump.dbspecificfeatures.grabextraconstraints";
	
	public static final String PROP_GRAB_FKFROMUK = "sqldump.dbspecificfeatures.grabfkfromuk";
	public static final String PROP_DUMP_SEQUENCE_STARTWITH = "sqldump.dbspecificfeatures.sequencestartwithdump";
	public static final String PROP_DUMP_TABLE_PHYSICAL_ATTRIBUTES = "sqldump.dbspecificfeatures.dumpphysicalattributes";
	public static final String PROP_DUMP_TABLE_LOGGING = "sqldump.dbspecificfeatures.dumplogging";
	public static final String PROP_DUMP_TABLE_PARTITION = "sqldump.dbspecificfeatures.dumppartition";
	
	public static final String PREFIX_DBMS = "sqldump.dbms";
	
	protected boolean grabExecutables = true;
	protected boolean grabIndexes = true;
	protected boolean grabSequences = true;
	protected boolean grabSynonyms = true;
	protected boolean grabTriggers = true;
	protected boolean grabViews = true;
	protected boolean grabMaterializedViews = true;
	protected boolean grabUniqueConstraints = true;
	protected boolean grabCheckConstraints = true;
	//XXX: protected boolean grabMaterializedViews = true;
	
	String id;
	
	public AbstractDBMSFeatures() {
	}
	
	public AbstractDBMSFeatures(String id) {
		this.id = id;
	}
	
	@Override
	public void setId(String id) {
		this.id = id;
	}
	
	@Override
	public String getId() {
		return id;
	}

	@Override
	public void procProperties(Properties prop) {
		grabIndexes = Utils.getPropBool(prop, PROP_GRAB_INDEXES, grabIndexes);
		grabExecutables = Utils.getPropBool(prop, PROP_GRAB_EXECUTABLES, grabExecutables);
		grabSequences = Utils.getPropBool(prop, PROP_GRAB_SEQUENCES, grabSequences);
		grabSynonyms = Utils.getPropBool(prop, PROP_GRAB_SYNONYMS, grabSynonyms);
		grabTriggers = Utils.getPropBool(prop, PROP_GRAB_TRIGGERS, grabTriggers);
		grabViews = Utils.getPropBool(prop, PROP_GRAB_VIEWS, grabViews);
		grabMaterializedViews = Utils.getPropBool(prop, PROP_GRAB_MATERIALIZED_VIEWS, grabMaterializedViews);
		grabUniqueConstraints = grabCheckConstraints = Utils.getPropBool(prop, PROP_GRAB_CONSTRAINTS_XTRA, grabUniqueConstraints);
	}
	
	@Override
	public DatabaseMetaData getMetadataDecorator(DatabaseMetaData metadata) throws SQLException {
		return metadata; //no decorator
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
	public void addTableSpecificFeatures(Table t, ResultSet rs) {
	}

	@Override
	public void addTableSpecificFeatures(Table t, Connection conn) throws SQLException {
	}

	@Override
	public void addColumnSpecificFeatures(Column c, ResultSet rs) {
	}

	@Override
	public void addFKSpecificFeatures(FK fk, ResultSet rs) {
	}
	
	@Override
	public Map<Class<?>, Class<?>> getColumnTypeMapper() {
		return null;
	}
	
	@Override
	public String sqlDefaultDateFormatPattern() {
		return null;
	}

	@Override
	public String sqlAlterColumnClause() {
		return "alter column";
	}
	
	@Override
	public String sqlAddColumnClause() {
		return "add column";
	}
	
	@Override
	public String sqlRenameColumnDefinition(NamedDBObject table, Column column, String newName) {
		//oracle & postgresql syntax
		return "alter table "+DBObject.getFinalName(table, true)+" rename column "+column.getName()+" to "+newName;
	}
	
	@Override
	public boolean supportsDiffingColumn() {
		return false;
	}
	
	@Override
	public boolean supportsAddColumnAfter() {
		return false;
	}
	
	@Override
	public boolean supportsCreateIndexWithoutName() {
		return false;
	}
	
	@Override
	public boolean alterColumnDefaultRequireFullDefinition() {
		return alterColumnTypeRequireFullDefinition();
	}
	
	@Override
	public boolean alterColumnNullableRequireFullDefinition() {
		return alterColumnTypeRequireFullDefinition();
	}
	
	@Override
	public boolean alterColumnTypeRequireFullDefinition() {
		return false;
	}
	
	/*@Override
	public String sqlAlterColumnByDiffing(NamedDBObject table, Column previousColumn, Column column) {
		throw new UnsupportedOperationException("can't sqlAlterColumnByDiffing()");
	}*/
	
	@Override
	public String sqlAlterColumnByDiffing(Column previousColumn, Column column) {
		throw new UnsupportedOperationException("can't sqlAlterColumnByDiffing()");
	}
	
	/*protected String createAlterColumn(NamedDBObject table, Column column, String xtraSql) {
		return "alter table "+DBObject.getFinalName(table, true)+" "+sqlAlterColumnClause()+" "+column.getName()
				+(xtraSql!=null?xtraSql:"");
	}*/
	
	@Override
	public String getIdentifierQuoteString() {
		return "\"";
	}
	
	@Override
	public List<ExecutableObject> grabExecutableNames(String catalog, String schema, String executableNamePattern, String[] types, Connection conn) throws SQLException {
		List<ExecutableObject> execs = new ArrayList<ExecutableObject>();
		grabDBExecutables(execs, schema, executableNamePattern, conn);
		return execs;
	}
	
	@Override
	public boolean sqlExceptionRequiresRollback() {
		return false;
	}
	
	@Override
	public String getExplainPlanQuery(String sql) {
		return sqlExplainPlanQuery(sql);
	}

	@Override
	public boolean supportsReleaseSavepoint() {
		return true;
	}

}
