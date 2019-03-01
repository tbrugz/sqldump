package tbrugz.sqldump.dbmd;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import tbrugz.sqldump.dbmodel.Column;
import tbrugz.sqldump.dbmodel.DBObjectType;
import tbrugz.sqldump.dbmodel.ExecutableObject;
import tbrugz.sqldump.dbmodel.FK;
import tbrugz.sqldump.dbmodel.NamedDBObject;
import tbrugz.sqldump.dbmodel.SchemaModel;
import tbrugz.sqldump.dbmodel.Sequence;
import tbrugz.sqldump.dbmodel.Synonym;
import tbrugz.sqldump.dbmodel.Table;
import tbrugz.sqldump.dbmodel.Trigger;
import tbrugz.sqldump.dbmodel.View;

public interface DBMSFeatures {
	void procProperties(Properties prop); //XXX: really needed?
	void grabDBObjects(SchemaModel model, String schemaPattern, Connection conn) throws SQLException; //XXX: remove this too?
	DatabaseMetaData getMetadataDecorator(DatabaseMetaData metadata);
	void setId(String id);
	String getId();
	
	/*
	//TODO: add standart methods for grabbing DB Objects - use 'get' instead of 'grab'?
	//XXX: add factory for specific DBMSFeatures?
	List<View> grabViews(String schemaPattern, String viewPattern) throws SQLException;
	List<Trigger> grabTriggers(String schemaPattern, String triggerPattern) throws SQLException;
	List<ExecutableObject> grabExecutables(String schemaPattern, String executablePattern) throws SQLException;
	////List<Sequence> grabDBSequences(SchemaModel model, String schemaPattern, Connection conn) throws SQLException;
	List<Sequence> grabSequences(String schemaPattern, String sequencePattern) throws SQLException;
	List<Constraint> grabCheckConstraints(String schemaPattern, String constraintPattern) throws SQLException;
	List<Constraint> grabUniqueConstraints(String schemaPattern, String constraintPattern) throws SQLException;
	//XXX: add Connection to parameters?
	
	boolean supportsGrabViews();
	boolean supportsGrabTriggers();
	boolean supportsGrabExecutables();
	boolean supportsGrabSequences();
	boolean supportsGrabSynonyms();
	boolean supportsGrabCheckConstraints();
	boolean supportsGrabUniqueConstraints();
	
	*/
	
	void addTableSpecificFeatures(Table t, ResultSet rs);
	void addTableSpecificFeatures(Table t, Connection conn) throws SQLException;
	void addColumnSpecificFeatures(Column c, ResultSet rs);
	void addFKSpecificFeatures(FK fk, ResultSet rs);
	
	Table getTableObject();
	FK getForeignKeyObject();
	Map<Class<?>, Class<?>> getColumnTypeMapper(); //XXX: remove getColumnTypeMapper()?
	//XXX: add Map<Integer, String> getSQLTypeClassMapper() (mainly for unknown types)?
	//XXX: add String getDefaultSchemaName()?
	
	List<DBObjectType> getExecutableObjectTypes();
	
	//DatabaseMetaData: supportsAlterTableWithAddColumn(), supportsAlterTableWithDropColumn()...
	
	String sqlAddColumnClause();
	String sqlAlterColumnClause();
	boolean supportsAddColumnAfter();
	
	//String sqlAlterColumnDefinition(NamedDBObject table, Column column); //removed...
	//String sqlAlterColumnNullableDefinition(NamedDBObject table, Column column);
	String sqlRenameColumnDefinition(NamedDBObject table, Column column, String newName);
	//XXXxx: should DBMS's Features return getDefaultDateFormat?
	String sqlDefaultDateFormatPattern();
	//String sqlDefaultTimestampFormatPattern();
	
	boolean supportsDiffingColumn();
	//String sqlAlterColumnByDiffing(NamedDBObject table, Column previousColumn, Column column);
	String sqlAlterColumnByDiffing(Column previousColumn, Column column);
	
	//boolean supportsRenameConstraint();
	//boolean supportsRenameIndex();
	
	//void grabDBViews(SchemaModel model, String schemaPattern, String viewNamePattern, Connection conn) throws SQLException;
	void grabDBViews(Collection<View> views, String schemaPattern, String viewNamePattern, Connection conn) throws SQLException;

	void grabDBMaterializedViews(Collection<View> views, String schemaPattern, String viewNamePattern, Connection conn) throws SQLException;
	
	//void grabDBTriggers(SchemaModel model, String schemaPattern, String tableNamePattern, String triggerNamePattern, Connection conn) throws SQLException;
	void grabDBTriggers(Collection<Trigger> triggers, String schemaPattern, String tableNamePattern, String triggerNamePattern, Connection conn) throws SQLException;
	
	//void grabDBExecutables(SchemaModel model, String schemaPattern, String execNamePattern, Connection conn) throws SQLException;
	void grabDBExecutables(Collection<ExecutableObject> execs, String schemaPattern, String execNamePattern, Connection conn) throws SQLException;
	
	//void grabDBSequences(SchemaModel model, String schemaPattern, String sequenceNamePattern, Connection conn) throws SQLException;
	void grabDBSequences(Collection<Sequence> seqs, String schemaPattern, String sequenceNamePattern, Connection conn) throws SQLException;

	//void grabDBSynonyms(SchemaModel model, String schemaPattern, String synonymNamePattern, Connection conn) throws SQLException;
	void grabDBSynonyms(Collection<Synonym> synonyms, String schemaPattern, String synonymNamePattern, Connection conn) throws SQLException;
	
	//void grabDBCheckConstraints(SchemaModel model, String schemaPattern, String constraintNamePattern, Connection conn) throws SQLException;
	void grabDBCheckConstraints(Collection<Table> tables, String schemaPattern, String constraintNamePattern, Connection conn) throws SQLException;
	
	//void grabDBUniqueConstraints(SchemaModel model, String schemaPattern, String constraintNamePattern, Connection conn) throws SQLException;
	void grabDBUniqueConstraints(Collection<Table> tables, String schemaPattern, String constraintNamePattern, Connection conn) throws SQLException;
	
	//XXX: grab views, executables (& triggers?) names
	//List<NamedDBObject> grabViewNames(String catalog, String schema, String viewNamePattern, Connection conn) throws SQLException;
	//List<NamedDBObject> grabTriggerNames(String catalog, String schema, String triggerNamePattern, Connection conn) throws SQLException;
	
	List<ExecutableObject> grabExecutableNames(String catalog, String schema, String executableNamePattern, String[] types, Connection conn) throws SQLException;
	
	boolean supportsExplainPlan();
	boolean supportsCreateIndexWithoutName();
	
	//String getExplainPlanForQuery(String sql);
	
	/**
	 * Returns the execution plan for a statement. Returned resultset column names are database-dependent (at least for now)
	 * 
	 * @param sql the query to explain
	 * @param conn the database connection
	 * @return a ResultSet with the plan explained
	 * @throws SQLException if a database access error occurs
	 */
	ResultSet explainPlan(String sql, List<Object> params, Connection conn) throws SQLException;
	
	String getExplainPlanQuery(String sql);
	
	/*
	 * http://stackoverflow.com/questions/3668506/efficient-sql-test-query-or-validation-query-that-will-work-across-all-or-most
	 * XXX add: boolean isValidConnection(Connection conn);
	 */
	
	String getIdentifierQuoteString();
	
	List<DBObjectType> getSupportedObjectTypes();
	
	boolean alterColumnTypeRequireFullDefinition();
	boolean alterColumnDefaultRequireFullDefinition();
	boolean alterColumnNullableRequireFullDefinition();

	boolean sqlExceptionRequiresRollback();
	
}
