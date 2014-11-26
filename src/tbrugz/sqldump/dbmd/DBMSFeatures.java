package tbrugz.sqldump.dbmd;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Collection;
import java.util.Map;
import java.util.Properties;

import tbrugz.sqldump.dbmodel.Column;
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
	boolean supportsGrabCheckConstraints();
	boolean supportsGrabUniqueConstraints();
	*/
	/*
	ResultSet getExplainPlanForQuery(String sql, Connection conn);
	boolean supportsExplainPlan();
	*/
	
	void addTableSpecificFeatures(Table t, ResultSet rs);
	void addColumnSpecificFeatures(Column c, ResultSet rs);
	void addFKSpecificFeatures(FK fk, ResultSet rs);
	
	Table getTableObject();
	FK getForeignKeyObject();
	Map<Class<?>, Class<?>> getColumnTypeMapper(); //XXX: remove getColumnTypeMapper()?
	//XXX: add Map<Integer, String> getSQLTypeClassMapper() (mainly for unknown types)?
	//XXX: add String getDefaultSchemaName()?
	
	//XXX: should DBMS's Features return getDefaultDateFormat?
	
	String sqlAddColumnClause();
	String sqlAlterColumnClause();
	//String sqlAlterColumnDefinition(NamedDBObject table, Column column); //removed...
	//String sqlAlterColumnNullableDefinition(NamedDBObject table, Column column);
	String sqlRenameColumnDefinition(NamedDBObject table, Column column, String newName);
	String sqlDefaultDateFormatPattern();
	
	boolean supportsDiffingColumn();
	String sqlAlterColumnByDiffing(NamedDBObject table, Column previousColumn, Column column);
	
	void grabDBViews(SchemaModel model, String schemaPattern, String viewNamePattern, Connection conn) throws SQLException;
	void grabDBViews(Collection<View> views, String schemaPattern, String viewNamePattern, Connection conn) throws SQLException;
	
	void grabDBTriggers(SchemaModel model, String schemaPattern, String tableNamePattern, String triggerNamePattern, Connection conn) throws SQLException;
	void grabDBTriggers(Collection<Trigger> triggers, String schemaPattern, String tableNamePattern, String triggerNamePattern, Connection conn) throws SQLException;
	
	void grabDBExecutables(SchemaModel model, String schemaPattern, String execNamePattern, Connection conn) throws SQLException;
	void grabDBExecutables(Collection<ExecutableObject> execs, String schemaPattern, String execNamePattern, Connection conn) throws SQLException;
	
	void grabDBSequences(SchemaModel model, String schemaPattern, String sequenceNamePattern, Connection conn) throws SQLException;
	void grabDBSequences(Collection<Sequence> seqs, String schemaPattern, String sequenceNamePattern, Connection conn) throws SQLException;

	void grabDBSynonyms(SchemaModel model, String schemaPattern, String synonymNamePattern, Connection conn) throws SQLException;
	void grabDBSynonyms(Collection<Synonym> synonyms, String schemaPattern, String synonymNamePattern, Connection conn) throws SQLException;
	
	void grabDBCheckConstraints(SchemaModel model, String schemaPattern, String constraintNamePattern, Connection conn) throws SQLException;
	void grabDBCheckConstraints(Collection<Table> constraints, String schemaPattern, String constraintNamePattern, Connection conn) throws SQLException;
	
	void grabDBUniqueConstraints(SchemaModel model, String schemaPattern, String constraintNamePattern, Connection conn) throws SQLException;
	void grabDBUniqueConstraints(Collection<Table> constraints, String schemaPattern, String constraintNamePattern, Connection conn) throws SQLException;
	
	//XXX: grab views, executables (& triggers?) names
	//List<NamedDBObject> grabViewNames(String catalog, String schema, String viewNamePattern, Connection conn) throws SQLException;
	//List<NamedDBObject> grabTriggerNames(String catalog, String schema, String triggerNamePattern, Connection conn) throws SQLException;
	//List<NamedDBObject> grabExecutableNames(String catalog, String schema, String executableNamePattern, Connection conn) throws SQLException;
	
}
