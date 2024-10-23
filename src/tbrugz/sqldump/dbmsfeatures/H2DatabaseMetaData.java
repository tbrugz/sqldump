package tbrugz.sqldump.dbmsfeatures;

import java.sql.DatabaseMetaData;

public class H2DatabaseMetaData extends InformationSchemaDatabaseMetaData {

	public H2DatabaseMetaData(DatabaseMetaData metadata) {
		super(metadata);
		xtraColumnInfoAvailable = true;
	}

}
