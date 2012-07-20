package tbrugz.sqldump.xtradumpers;

import java.io.IOException;
import java.util.Properties;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import tbrugz.sqldump.SchemaModelScriptDumper;
import tbrugz.sqldump.dbmodel.DBObjectType;
import tbrugz.sqldump.dbmodel.FK;
import tbrugz.sqldump.dbmodel.SchemaModel;
import tbrugz.sqldump.def.SchemaModelDumper;
import tbrugz.sqldump.util.CategorizedOut;

//TODO: prop for setting types of object to dump; optional dump scripts in one file for each type (categorizedOut?)
public class DropScriptDumper implements SchemaModelDumper {
	
	static final Log log = LogFactory.getLog(DropScriptDumper.class);
	
	static final String PROP_PREFIX = "sqldump.dropscriptdumper";
	static final String PROP_OUTFILEPATTERN = PROP_PREFIX+".outfilepattern";
	
	String outfilePattern = null;

	@Override
	public void procProperties(Properties prop) {
		outfilePattern = prop.getProperty(PROP_OUTFILEPATTERN);
		if(outfilePattern==null) {
			log.warn("outfilepattern not defined [prop '"+PROP_OUTFILEPATTERN+"']. can't dump drop script");
			return;
		}
	}

	@Override
	public void setPropertiesPrefix(String propertiesPrefix) {
	}

	//TODO: drop tables, indexes
	@Override
	public void dumpSchema(SchemaModel schemaModel) {
		try {
			if(outfilePattern==null) { return; }
			
			CategorizedOut co = new CategorizedOut();
			String finalPattern = outfilePattern.replaceAll(SchemaModelScriptDumper.FILENAME_PATTERN_SCHEMA, "\\$\\{1\\}")
					.replaceAll(SchemaModelScriptDumper.FILENAME_PATTERN_OBJECTTYPE, "\\$\\{2\\}");
			co.setFilePathPattern(finalPattern);
			
			dumpDropFKs(schemaModel, co);
			//drop indexes
			//drop tables
		}
		catch (IOException e) {
			log.warn("i/o error dumping drop script: "+e);
		}
	}
	
	public void dumpDropFKs(SchemaModel schemaModel, CategorizedOut co) throws IOException {
		String fkcat = DBObjectType.FK.toString();
		for(FK fk: schemaModel.getForeignKeys()) {
			String script = "alter table "+fk.fkTable+" drop constraint "+fk.getName()+";\n";
			co.categorizedOut(script, fk.getSchemaName(), fkcat);
		}
	}
	
}
