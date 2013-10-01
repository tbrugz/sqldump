package tbrugz.sqldump.dbmd;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Map;
import java.util.Properties;

import tbrugz.sqldump.dbmodel.Column;
import tbrugz.sqldump.dbmodel.FK;
import tbrugz.sqldump.dbmodel.NamedDBObject;
import tbrugz.sqldump.dbmodel.SchemaModel;
import tbrugz.sqldump.dbmodel.Table;

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
	
	boolean supportsDiffingColumn();
	String sqlAlterColumnByDiffing(NamedDBObject table, Column previousColumn, Column column);
}