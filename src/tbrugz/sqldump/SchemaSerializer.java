package tbrugz.sqldump;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.sql.Connection;
import java.util.Properties;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import tbrugz.sqldump.dbmodel.SchemaModel;
import tbrugz.sqldump.def.AbstractFailable;
import tbrugz.sqldump.def.SchemaModelDumper;
import tbrugz.sqldump.def.SchemaModelGrabber;

public class SchemaSerializer extends AbstractFailable implements SchemaModelDumper, SchemaModelGrabber {

	static Log log = LogFactory.getLog(SchemaSerializer.class);
	
	public static final String SERIALIZATION_DEFAULT_PREFIX = "sqldump.serialization";
	public static final String PROP_SERIALIZATION_OUTFILE = SERIALIZATION_DEFAULT_PREFIX + ".outfile";
	public static final String PROP_SERIALIZATION_INFILE = SERIALIZATION_DEFAULT_PREFIX + ".infile";
	public static final String PROP_SERIALIZATION_INRESOURCE = SERIALIZATION_DEFAULT_PREFIX + ".inresource";

	String filenameIn;
	InputStream fileInput;
	String fileOutput;
	
	@Override
	public void procProperties(Properties prop) {
		fileOutput = prop.getProperty(PROP_SERIALIZATION_OUTFILE);
		String fileInputStr = prop.getProperty(PROP_SERIALIZATION_INFILE);
		if(fileInputStr==null) {
			fileInputStr = prop.getProperty(PROP_SERIALIZATION_INRESOURCE);
			if(fileInputStr!=null) {
				filenameIn = fileInputStr;
				fileInput = JAXBSchemaXMLSerializer.class.getResourceAsStream(fileInputStr);
			}
		}
		else {
			try {
				filenameIn = fileInputStr;
				fileInput = new FileInputStream(new File(fileInputStr));
			} catch (FileNotFoundException e) {
				log.warn("procproperties: File not found: "+fileInputStr);
			}
		}
	}
	
	@Override
	public void dumpSchema(SchemaModel schemaModel) {
		if(fileOutput==null) {
			log.error("serialization output file ["+PROP_SERIALIZATION_OUTFILE+"] not defined");
			if(failonerror) { throw new ProcessingException("SchemaSerializer: serialization output file ["+PROP_SERIALIZATION_OUTFILE+"] not defined"); }
			return;
		}
		
		try {
			ObjectOutputStream oos = new ObjectOutputStream(new FileOutputStream(fileOutput));
			oos.writeObject(schemaModel);
			oos.close();
			log.info("schema model serialized to '"+fileOutput+"'");
		}
		catch(IOException e) {
			log.error("error dumping schema: "+e);
			log.debug("error dumping schema", e);
			if(failonerror) { throw new ProcessingException(e); }
		}
	}

	@Override
	public SchemaModel grabSchema() {
		if(fileInput==null) {
			log.error("serialization input file ["+PROP_SERIALIZATION_INFILE+"] not defined");
			if(failonerror) { throw new ProcessingException("SchemaSerializer: serialization input file ["+PROP_SERIALIZATION_INFILE+"] not defined"); }
			return null;
		}

		try {
			ObjectInputStream ois = new ObjectInputStream(fileInput);
			SchemaModel sm = (SchemaModel) ois.readObject();
			ois.close();
			log.info("serialized schema model grabbed from '"+fileInput+"'");
			return sm;
		}
		catch (ClassNotFoundException e) {
			log.error("error grabbing schema: "+e);
			log.debug("error grabbing schema", e);
			if(failonerror) { throw new ProcessingException(e); }
			return null;
		}
		catch(IOException e) {
			log.error("error grabbing schema: "+e);
			log.debug("error grabbing schema", e);
			if(failonerror) { throw new ProcessingException(e); }
			return null;
		}
	}

	@Override
	public void setConnection(Connection conn) {
		log.debug("setConnection() is empty");
	}

	@Override
	public boolean needsConnection() {
		return false;
	}

	@Override
	public void setPropertiesPrefix(String propertiesPrefix) {
		// TODO: properties-prefix setting
	}
}
