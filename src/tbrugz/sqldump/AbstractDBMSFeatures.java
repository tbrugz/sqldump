package tbrugz.sqldump;

import java.sql.DatabaseMetaData;

public abstract class AbstractDBMSFeatures implements DBMSFeatures {

	public DatabaseMetaData getMetadataDecorator(DatabaseMetaData metadata) {
		return metadata; //no decorator
	}

}
