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
import tbrugz.sqldump.def.AbstractModelDumper;
import tbrugz.sqldump.def.ProcessingException;
import tbrugz.sqldump.def.SchemaModelDumper;
import tbrugz.sqldump.util.CategorizedOut;
import tbrugz.sqldump.util.Utils;

/*
 * TODO: prop for setting types of object to dump
 * TODOne: optional dump scripts in one file for each type (categorizedOut?)
 * TODO: option to output schemaName
 */
public class DropScriptDumper extends AbstractModelDumper implements SchemaModelDumper {
	
	static final Log log = LogFactory.getLog(DropScriptDumper.class);
	
	static final String PROP_PREFIX = "sqldump.dropscriptdumper";
	static final String PROP_OUTFILEPATTERN = PROP_PREFIX+".outfilepattern";
	static final String PROP_USE_IFEXISTS = PROP_PREFIX+".ifexists";
	
	String outfilePattern = null;
	boolean useIfExists = false;

	@Override
	public void setProperties(Properties prop) {
		outfilePattern = prop.getProperty(PROP_OUTFILEPATTERN);
		useIfExists = Utils.getPropBool(prop, PROP_USE_IFEXISTS, useIfExists);
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
			
			int dropCount = 0;
			dropCount += dumpDropFKs(schemaModel, useIfExists, co);
			dropCount += dumpDropObject(DBObjectType.INDEX.desc(), schemaModel.getIndexes(), useIfExists, co);
			dropCount += dumpDropObject(DBObjectType.VIEW.desc(), schemaModel.getViews(), useIfExists, co);
			dropCount += dumpDropObject(DBObjectType.SEQUENCE.desc(), schemaModel.getSequences(), useIfExists, co);
			//XXX: dump drop executables/triggers
			//XXX: dump truncate tables?
			dropCount += dumpDropObject(DBObjectType.TABLE.desc(), schemaModel.getTables(), useIfExists, co);
			log.info(dropCount + " drop scripts dumped [output pattern: "+finalPattern+"]");
		}
		catch (IOException e) {
			log.error("i/o error dumping drop script: "+e);
			if(failonerror) { throw new ProcessingException(e); }
		}
	}
	
	public int dumpDropFKs(SchemaModel schemaModel, boolean ifExists, CategorizedOut co) throws IOException {
		int count = 0;
		String fkcat = DBObjectType.FK.toString();
		for(FK fk: schemaModel.getForeignKeys()) {
			String script = "alter table "+fk.getFkTable()+" drop constraint "+
					(ifExists?"if exists ":"")+
					fk.getName()+";\n";
			co.categorizedOut(script, fk.getSchemaName(), fkcat);
			count++;
		}
		return count;
	}
	
	public int dumpDropObject(String objectType, Collection<? extends DBObject> objects, boolean ifExists, CategorizedOut co) throws IOException {
		int count = 0;
		for(DBObject obj: objects) {
			String script = "drop "+objectType+" "+
					(ifExists?"if exists ":"")+
					obj.getName()+";\n";
			co.categorizedOut(script, obj.getSchemaName(), objectType);
			count++;
		}
		return count;
	}
	
	@Override
	public String getMimeType() {
		return SQL_MIME_TYPE;
	}
}
