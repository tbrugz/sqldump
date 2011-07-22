package tbrugz.sqldump;

import java.io.File;
import java.util.Properties;

import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBException;
import javax.xml.bind.Marshaller;
import javax.xml.bind.Unmarshaller;

import org.apache.log4j.Logger;

public class JAXBSchemaXMLSerializer implements SchemaModelDumper, SchemaModelGrabber {

	static Logger log = Logger.getLogger(JAXBSchemaXMLSerializer.class);
	
	public static final String PROP_XMLSERIALIZATION_JAXB_OUTFILE = "sqldump.xmlserialization.jaxb.outfile";
	public static final String PROP_XMLSERIALIZATION_JAXB_INFILE = "sqldump.xmlserialization.jaxb.infile";

	String fileInput;
	String fileOutput;
	JAXBContext jc;
	
	public JAXBSchemaXMLSerializer() {
		try {
			//jc = JAXBContext.newInstance( "tbrugz.sqldump" );
			jc = JAXBContext.newInstance( "tbrugz.sqldump:tbrugz.sqldump.dbmodel:tbrugz.sqldump.dbmsfeatures" );
		} catch (JAXBException e) {
			log.warn("impossible to create JAXBContext: "+e);
			e.printStackTrace();
		}
	}
	
	@Override
	public void procProperties(Properties prop) {
		fileOutput = prop.getProperty(PROP_XMLSERIALIZATION_JAXB_OUTFILE);
		fileInput = prop.getProperty(PROP_XMLSERIALIZATION_JAXB_INFILE);
	}
	
	@Override
	public void dumpSchema(SchemaModel schemaModel) throws Exception {
		if(fileOutput==null) {
			log.warn("xml serialization output file ["+PROP_XMLSERIALIZATION_JAXB_OUTFILE+"] not defined");
			return;
		}

		Marshaller m = jc.createMarshaller();
		//XXX: property for formatting or not JAXB output?
		//see: http://ws.apache.org/jaxme/release-0.3/manual/ch02s02.html
		m.setProperty(Marshaller.JAXB_FORMATTED_OUTPUT, true);		
		m.marshal(schemaModel, new File(fileOutput));
	}

	@Override
	public SchemaModel grabSchema() throws Exception {
		if(fileInput==null) {
			log.warn("xml serialization input file ["+PROP_XMLSERIALIZATION_JAXB_INFILE+"] not defined");
			return null;
		}
		
		Unmarshaller u = jc.createUnmarshaller();
		return (SchemaModel) u.unmarshal(new File(fileInput));
	}

}
