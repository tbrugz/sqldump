package tbrugz.sqldump;

import java.io.FileReader;
import java.io.FileWriter;
import java.sql.Connection;
import java.util.Properties;

import org.apache.log4j.Logger;
import org.exolab.castor.xml.Marshaller;
import org.exolab.castor.xml.Unmarshaller;

public class CastorSchemaXMLSerializer implements SchemaModelDumper, SchemaModelGrabber {

	static Logger log = Logger.getLogger(CastorSchemaXMLSerializer.class);
	
	public static final String PROP_XMLSERIALIZATION_CASTOR_OUTFILE = "sqldump.xmlserialization.castor.outfile";
	public static final String PROP_XMLSERIALIZATION_CASTOR_INFILE = "sqldump.xmlserialization.castor.infile";

	String fileInput;
	String fileOutput;
	
	@Override
	public void procProperties(Properties prop) {
		fileOutput = prop.getProperty(PROP_XMLSERIALIZATION_CASTOR_OUTFILE);
		fileInput = prop.getProperty(PROP_XMLSERIALIZATION_CASTOR_INFILE);
	}
	
	@Override
	public void dumpSchema(SchemaModel schemaModel) throws Exception {
		if(fileOutput==null) {
			log.warn("xml serialization output file ["+PROP_XMLSERIALIZATION_CASTOR_OUTFILE+"] not defined");
			return;
		}
		
		Marshaller.marshal(schemaModel, new FileWriter(fileOutput));
	}

	@Override
	public SchemaModel grabSchema() throws Exception {
		if(fileInput==null) {
			log.warn("xml serialization input file ["+PROP_XMLSERIALIZATION_CASTOR_INFILE+"] not defined");
			return null;
		}
		
		return (SchemaModel) Unmarshaller.unmarshal(SchemaModel.class, new FileReader(fileInput));
	}

	@Override
	public void setConnection(Connection conn) {
		log.debug("setConnection() is empty");
	}

}
