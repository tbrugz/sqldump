package tbrugz.sqldump.def;

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
	static String PROP_GRAB_INDEXES = "sqldump.dbspecificfeatures.grabindexes";
	static String PROP_GRAB_EXECUTABLES = "sqldump.dbspecificfeatures.grabexecutables";
	static String PROP_GRAB_VIEWS = "sqldump.dbspecificfeatures.grabviews";
	static String PROP_GRAB_TRIGGERS = "sqldump.dbspecificfeatures.grabtriggers";
	static String PROP_GRAB_SYNONYMS = "sqldump.dbspecificfeatures.grabsynonyms";
	static String PROP_GRAB_SEQUENCES = "sqldump.dbspecificfeatures.grabsequences";
	static String PROP_GRAB_CONSTRAINTS_XTRA = "sqldump.dbspecificfeatures.grabextraconstraints";
	
	static String PROP_GRAB_FKFROMUK = "sqldump.dbspecificfeatures.grabfkfromuk";
	static String PROP_DUMP_SEQUENCE_STARTWITH = "sqldump.dbspecificfeatures.sequencestartwithdump";
	static String PROP_DUMP_TABLE_PHYSICAL_ATTRIBUTES = "sqldump.dbspecificfeatures.dumpphysicalattributes";
	static String PROP_DUMP_TABLE_LOGGING = "sqldump.dbspecificfeatures.dumplogging";
	static String PROP_DUMP_TABLE_PARTITION = "sqldump.dbspecificfeatures.dumppartition";
	
	void procProperties(Properties prop);
	void grabDBObjects(SchemaModel model, String schemaPattern,	Connection conn) throws SQLException;
	DatabaseMetaData getMetadataDecorator(DatabaseMetaData metadata);
	
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