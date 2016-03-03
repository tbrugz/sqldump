package tbrugz.sqldump.dbmsfeatures;

import java.sql.DatabaseMetaData;
import java.sql.ResultSet;

import tbrugz.sqldump.dbmodel.Column;
import tbrugz.sqldump.dbmodel.FK;
import tbrugz.sqldump.dbmodel.Table;

/**
 * Does not create decorator for DatabaseMetaData - use driver plain methods
 */
public class OracleFeaturesLite extends OracleFeatures {

	@Override
	public DatabaseMetaData getMetadataDecorator(DatabaseMetaData metadata) {
		return metadata;
	}
	
	/*
	 * not adding column specific features since this class does not use OracleDatabaseMetaData
	 */
	@Override
	public void addColumnSpecificFeatures(Column c, ResultSet rs) {
	}
	
	/*
	 * not adding table specific features since this class does not use OracleDatabaseMetaData
	 */
	@Override
	public void addTableSpecificFeatures(Table t, ResultSet rs) {
	}
	
	/*
	 * not adding FK specific features since this class does not use OracleDatabaseMetaData
	 */
	@Override
	public void addFKSpecificFeatures(FK fk, ResultSet rs) {
	}

}
