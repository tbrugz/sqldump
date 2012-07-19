package tbrugz.sqldump.xtradumpers;

import java.io.FileWriter;
import java.io.IOException;
import java.util.Properties;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import tbrugz.sqldump.dbmodel.FK;
import tbrugz.sqldump.dbmodel.SchemaModel;
import tbrugz.sqldump.def.SchemaModelDumper;

//TODO: prop for setting types of object to dump; optional dump scripts in one file for each type (categorizedOut?)
public class DropScriptDumper implements SchemaModelDumper {
	
	static final Log log = LogFactory.getLog(DropScriptDumper.class);
	
	static final String PROP_PREFIX = "sqldump.dropscriptdumper";
	static final String PROP_OUTFILE = PROP_PREFIX+".outfile";
	
	String outfile = null;
	FileWriter fw = null;
	boolean invalid = true;

	@Override
	public void procProperties(Properties prop) {
		outfile = prop.getProperty(PROP_OUTFILE);
		if(outfile==null) {
			log.warn("outfile not defined [prop '"+PROP_OUTFILE+"']. can't dump drop script");
			return;
		}
		
		/*try {
		} catch (IOException e) {
			fw = new FileWriter(outfile);
			invalid = false;
			log.warn("error opening file (rw): "+outfile);
		}*/
	}

	@Override
	public void setPropertiesPrefix(String propertiesPrefix) {
	}

	//TODO: drop tables, indexes
	@Override
	public void dumpSchema(SchemaModel schemaModel) {
		try {
			fw = new FileWriter(outfile);
			invalid = false;
			
			dumpDropFKs(schemaModel);
			
			fw.close();
		} catch (IOException e) {
			log.warn("i/o error dumping drop script: "+e);
		}
	}
	
	public void dumpDropFKs(SchemaModel schemaModel) {
		try {
			for(FK fk: schemaModel.getForeignKeys()) {
				String script = "alter table "+fk.fkTable+" drop constraint "+fk.getName()+";\n";
				out(script);
			}
		}
		catch(IOException e) {
			log.warn("error dumping drop script: "+e);
		}
	}
	
	void out(String str) throws IOException {
		if(!invalid) { fw.write(str); }
	}
	
}
