package tbrugz.sqldump;

import java.io.IOException;

import tbrugz.sqldump.dbmodel.DBObjectType;
import tbrugz.sqldump.dbmodel.FK;
import tbrugz.sqldump.dbmodel.SchemaModel;
import tbrugz.sqldump.def.ProcessingException;

public class FKScriptDumper extends SchemaModelScriptDumper {

	@Override
	public void dumpSchema(SchemaModel schemaModel) {
		try {
			dumpSchemaInternal(schemaModel);
		} catch (IOException e) {
			log.warn("error: "+e);
			if(failonerror) {
				throw new ProcessingException(e);
			}
		}
	}
	
	void dumpSchemaInternal(SchemaModel schemaModel) throws IOException {
		//FKs
		for(FK fk: schemaModel.getForeignKeys()) {
			String fkscript = fk.fkScriptWithAlterTable(dumpDropStatements, dumpWithSchemaName);
			categorizedOut(fk.getSchemaName(), fk.getName(), DBObjectType.FK, fkscript);
		}
	}
	
}
