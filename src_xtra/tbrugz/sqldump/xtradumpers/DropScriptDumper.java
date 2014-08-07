package tbrugz.sqldump.xtradumpers;

import java.io.IOException;
import java.util.Collection;
import java.util.Properties;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import tbrugz.sqldump.dbmodel.DBObject;
import tbrugz.sqldump.dbmodel.DBObjectType;
import tbrugz.sqldump.dbmodel.FK;
import tbrugz.sqldump.dbmodel.SchemaModel;
import tbrugz.sqldump.def.AbstractFailable;
import tbrugz.sqldump.def.ProcessingException;
import tbrugz.sqldump.def.SchemaModelDumper;
import tbrugz.sqldump.util.CategorizedOut;

/*
 * TODO: prop for setting types of object to dump
 * TODOne: optional dump scripts in one file for each type (categorizedOut?)
 * TODO: option to output schemaName
 */
public class DropScriptDumper extends AbstractFailable implements SchemaModelDumper {
	
	static final Log log = LogFactory.getLog(DropScriptDumper.class);
	
	static final String PROP_PREFIX = "sqldump.dropscriptdumper";
	static final String PROP_OUTFILEPATTERN = PROP_PREFIX+".outfilepattern";
	
	String outfilePattern = null;

	@Override
	public void setProperties(Properties prop) {
		outfilePattern = prop.getProperty(PROP_OUTFILEPATTERN);
		if(outfilePattern==null) {
			log.warn("outfilepattern not defined [prop '"+PROP_OUTFILEPATTERN+"']. can't dump drop script");
			return;
		}
	}

	@Override
	public void setPropertiesPrefix(String propertiesPrefix) {
	}

	@Override
	public void dumpSchema(SchemaModel schemaModel) {
		try {
			if(outfilePattern==null) {
				if(failonerror) { throw new ProcessingException("outfilepattern is null"); }
				return;
			}
			
			String finalPattern = CategorizedOut.generateFinalOutPattern(outfilePattern,
					AlterSchemaSuggester.FILENAME_PATTERN_SCHEMA,
					AlterSchemaSuggester.FILENAME_PATTERN_OBJECTTYPE);
			//XXX: Matcher.quoteReplacement()? maybe not...
			CategorizedOut co = new CategorizedOut(finalPattern);
			
			dumpDropFKs(schemaModel, co);
			dumpDropObject(DBObjectType.INDEX.toString(), schemaModel.getIndexes(), co);
			dumpDropObject(DBObjectType.VIEW.toString(), schemaModel.getViews(), co);
			dumpDropObject(DBObjectType.SEQUENCE.toString(), schemaModel.getSequences(), co);
			//XXX: dump drop executables/triggers
			//XXX: dump truncate tables?
			dumpDropObject(DBObjectType.TABLE.toString(), schemaModel.getTables(), co);
		}
		catch (IOException e) {
			log.error("i/o error dumping drop script: "+e);
			if(failonerror) { throw new ProcessingException(e); }
		}
	}
	
	public void dumpDropFKs(SchemaModel schemaModel, CategorizedOut co) throws IOException {
		String fkcat = DBObjectType.FK.toString();
		for(FK fk: schemaModel.getForeignKeys()) {
			String script = "alter table "+fk.getFkTable()+" drop constraint "+fk.getName()+";\n";
			co.categorizedOut(script, fk.getSchemaName(), fkcat);
		}
	}
	
	public void dumpDropObject(String objectType, Collection<? extends DBObject> objects, CategorizedOut co) throws IOException {
		for(DBObject obj: objects) {
			String script = "drop "+objectType+" "+obj.getName()+";\n";
			co.categorizedOut(script, obj.getSchemaName(), objectType);
		}
	}
	
	@Override
	public String getMimeType() {
		return SQL_MIME_TYPE;
	}
}
