package tbrugz.sqldump;

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.sql.Connection;
import java.util.Properties;

import org.apache.log4j.Logger;

public class SchemaSerializer implements SchemaModelDumper, SchemaModelGrabber {

	static Logger log = Logger.getLogger(SchemaSerializer.class);
	
	public static final String PROP_SERIALIZATION_OUTFILE = "sqldump.serialization.outfile";
	public static final String PROP_SERIALIZATION_INFILE = "sqldump.serialization.infile";

	String fileInput;
	String fileOutput;
	
	@Override
	public void procProperties(Properties prop) {
		fileOutput = prop.getProperty(PROP_SERIALIZATION_OUTFILE);
		fileInput = prop.getProperty(PROP_SERIALIZATION_INFILE);
	}
	
	@Override
	public void dumpSchema(SchemaModel schemaModel) throws Exception {
		if(fileOutput==null) {
			log.warn("serialization output file ["+PROP_SERIALIZATION_OUTFILE+"] not defined");
			return;
		}
		
		ObjectOutputStream oos = new ObjectOutputStream(new FileOutputStream(fileOutput));
		oos.writeObject(schemaModel);
		oos.close();
		log.info("schema model serialized to '"+fileOutput+"'");
	}

	@Override
	public SchemaModel grabSchema() throws Exception {
		if(fileInput==null) {
			log.warn("serialization input file ["+PROP_SERIALIZATION_INFILE+"] not defined");
			return null;
		}
		
		ObjectInputStream ois = new ObjectInputStream(new FileInputStream(fileInput));
		SchemaModel sm = (SchemaModel) ois.readObject();
		ois.close();
		log.info("serialized schema model grabbed from '"+fileInput+"'");
		return sm;
	}

	@Override
	public void setConnection(Connection conn) {
		log.debug("setConnection() is empty");
	}

	@Override
	public boolean needsConnection() {
		return false;
	}
	
}
