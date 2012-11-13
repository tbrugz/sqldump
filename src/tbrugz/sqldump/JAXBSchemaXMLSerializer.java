package tbrugz.sqldump;

import java.io.File;
import java.sql.Connection;
import java.util.Properties;

import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBException;
import javax.xml.bind.Marshaller;
import javax.xml.bind.Unmarshaller;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import tbrugz.sqldump.dbmodel.SchemaModel;
import tbrugz.sqldump.dbmodel.Table;
import tbrugz.sqldump.def.SchemaModelDumper;
import tbrugz.sqldump.def.SchemaModelGrabber;

public class JAXBSchemaXMLSerializer implements SchemaModelDumper, SchemaModelGrabber {

	static Log log = LogFactory.getLog(JAXBSchemaXMLSerializer.class);
	
	public static final String XMLSERIALIZATION_JAXB_DEFAULT_PREFIX = "sqldump.xmlserialization.jaxb";
	public static final String PROP_XMLSERIALIZATION_JAXB_OUTFILE = ".outfile";
	public static final String PROP_XMLSERIALIZATION_JAXB_INFILE = ".infile";
	public static final String PROP_XMLSERIALIZATION_JAXB_INRESOURCE = ".inresource";

	String propertiesPrefix = XMLSERIALIZATION_JAXB_DEFAULT_PREFIX;
	
	File fileInput;
	String fileOutput;
	JAXBContext jc;
	
	public JAXBSchemaXMLSerializer() {
		try {
			//jc = JAXBContext.newInstance( "tbrugz.sqldump" );
			jc = JAXBContext.newInstance( "tbrugz.sqldump.dbmodel:tbrugz.sqldump.dbmsfeatures" );
		} catch (JAXBException e) {
			log.warn("impossible to create JAXBContext: "+e);
			e.printStackTrace();
		}
	}
	
	@Override
	public void procProperties(Properties prop) {
		fileOutput = prop.getProperty(propertiesPrefix+PROP_XMLSERIALIZATION_JAXB_OUTFILE);
		String fileInputStr = prop.getProperty(propertiesPrefix+PROP_XMLSERIALIZATION_JAXB_INFILE);
		if(fileInputStr==null) {
			fileInputStr = prop.getProperty(propertiesPrefix+PROP_XMLSERIALIZATION_JAXB_INRESOURCE);
			if(fileInputStr!=null) {
				fileInput = new File(JAXBSchemaXMLSerializer.class.getResource(fileInputStr).getFile());
			}
		}
		else {
			fileInput = new File(fileInputStr);
		}
	}
	
	@Override
	public void dumpSchema(SchemaModel schemaModel) {
		if(fileOutput==null) {
			log.warn("xml serialization output file ["+propertiesPrefix+PROP_XMLSERIALIZATION_JAXB_OUTFILE+"] not defined");
			return;
		}
		if(schemaModel==null) {
			log.warn("schemaModel is null!");
			return;
		}

		try {
			Marshaller m = jc.createMarshaller();
			//XXX: property for formatting or not JAXB output?
			//see: http://ws.apache.org/jaxme/release-0.3/manual/ch02s02.html
			m.setProperty(Marshaller.JAXB_FORMATTED_OUTPUT, true);		
			m.marshal(schemaModel, new File(fileOutput));
			log.info("xml schema model dumped to '"+fileOutput+"'");
		}
		catch(Exception e) {
			log.warn("error dumping schema: "+e);
			log.debug("error dumping schema", e);
		}
	}

	@Override
	public SchemaModel grabSchema() {
		if(fileInput==null) {
			log.warn("xml serialization input file ["+propertiesPrefix+PROP_XMLSERIALIZATION_JAXB_INFILE+"] not defined");
			return null;
		}

		try {
			Unmarshaller u = jc.createUnmarshaller();
			SchemaModel sm = (SchemaModel) u.unmarshal(fileInput);
			//use Unmarshaller.afterUnmarshal()?
			for(Table t: sm.getTables()) {
				t.validateConstraints();
			}
			log.info("xml schema model grabbed from '"+fileInput+"'");
			return sm;
		}
		catch(Exception e) {
			log.warn("error grabbing schema: "+e);
			log.debug("error grabbing schema", e);
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

	public String getPropertiesPrefix() {
		return propertiesPrefix;
	}

	@Override
	public void setPropertiesPrefix(String propertiesPrefix) {
		this.propertiesPrefix = propertiesPrefix;
	}

}
