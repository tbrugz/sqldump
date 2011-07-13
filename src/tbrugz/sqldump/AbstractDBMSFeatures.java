package tbrugz.sqldump;

import java.sql.DatabaseMetaData;
import java.sql.ResultSet;

import tbrugz.sqldump.dbmodel.Table;

public abstract class AbstractDBMSFeatures implements DBMSFeatures {

	public DatabaseMetaData getMetadataDecorator(DatabaseMetaData metadata) {
		return metadata; //no decorator
	}

	public void addTableSpecificFeatures(Table t, ResultSet rs) {
	}
}
