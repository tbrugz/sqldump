package tbrugz.sqldump.def;

import tbrugz.sqldump.dbmodel.SchemaModel;

public interface SchemaModelDumper extends ProcessComponent {

	//XXX return errorlevel? not much java-like...
	public void dumpSchema(SchemaModel schemaModel);

}