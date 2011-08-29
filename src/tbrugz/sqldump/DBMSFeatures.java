package tbrugz.sqldump;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Properties;

import tbrugz.sqldump.dbmodel.Column;
import tbrugz.sqldump.dbmodel.Table;

public interface DBMSFeatures {
	static String PROP_GRAB_INDEXES = "sqldump.dbspecificfeatures.grabindexes";
	static String PROP_GRAB_EXECUTABLES = "sqldump.dbspecificfeatures.grabexecutables";
	static String PROP_GRAB_VIEWS = "sqldump.dbspecificfeatures.grabviews";
	static String PROP_GRAB_TRIGGERS = "sqldump.dbspecificfeatures.grabtriggers";
	static String PROP_GRAB_SYNONYMS = "sqldump.dbspecificfeatures.grabsynonyms";
	static String PROP_GRAB_SEQUENCES = "sqldump.dbspecificfeatures.grabsequences";
	
	static String PROP_GRAB_FKFROMUK = "sqldump.dbspecificfeatures.grabfkfromuk";
	static String PROP_SEQUENCE_STARTWITHDUMP = "sqldump.dbspecificfeatures.sequencestartwithdump";
	
	void procProperties(Properties prop);
	void grabDBObjects(SchemaModel model, String schemaPattern,	Connection conn) throws SQLException;
	DatabaseMetaData getMetadataDecorator(DatabaseMetaData metadata);
	void addTableSpecificFeatures(Table t, ResultSet rs);
	void addColumnSpecificFeatures(Column c, ResultSet rs);
	Table getTableObject();
	//XXX: should DBMS's Features return getDefaultDateFormat?
}