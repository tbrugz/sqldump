package tbrugz.sqldump.dbmsfeatures;

import java.sql.DatabaseMetaData;

/**
 * Does not create decorator for DatabaseMetaData - use driver plain methods
 */
public class OracleFeaturesLite extends OracleFeatures {

	@Override
	public DatabaseMetaData getMetadataDecorator(DatabaseMetaData metadata) {
		return metadata;
	}
}
