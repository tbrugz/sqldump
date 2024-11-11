package tbrugz.sqldump.def;

import tbrugz.sqldump.dbmodel.SchemaModel;

public abstract class AbstractSchemaProcessor extends AbstractProcessor {

	protected SchemaModel model;
	
	@Override
	public void setSchemaModel(SchemaModel schemamodel) {
		this.model = schemamodel;
	}
	
	@Override
	public boolean needsSchemaModel() {
		return true;
	}
}
