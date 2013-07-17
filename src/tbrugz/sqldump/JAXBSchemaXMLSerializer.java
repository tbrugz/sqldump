package tbrugz.sqldump;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.sql.Connection;
import java.util.Properties;

import javax.xml.bind.JAXBException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import tbrugz.sqldump.dbmodel.SchemaModel;
import tbrugz.sqldump.dbmodel.Table;
import tbrugz.sqldump.def.AbstractFailable;
import tbrugz.sqldump.def.ProcessingException;
import tbrugz.sqldump.def.SchemaModelDumper;
import tbrugz.sqldump.def.SchemaModelGrabber;
import tbrugz.sqldump.util.XMLSerializer;

public class JAXBSchemaXMLSerializer extends AbstractFailable implements SchemaModelDumper, SchemaModelGrabber {

	static final Log log = LogFactory.getLog(JAXBSchemaXMLSerializer.class);
	
	public static final String XMLSERIALIZATION_JAXB_DEFAULT_PREFIX = "sqldump.xmlserialization.jaxb";
	public static final String PROP_XMLSERIALIZATION_JAXB_OUTFILE = ".outfile";
	public static final String PROP_XMLSERIALIZATION_JAXB_INFILE = ".infile";
	public static final String PROP_XMLSERIALIZATION_JAXB_INRESOURCE = ".inresource";
	
	static final String JAXB_SCHEMA_PACKAGES = "tbrugz.sqldump.dbmodel:tbrugz.sqldump.dbmsfeatures";

	String propertiesPrefix = XMLSERIALIZATION_JAXB_DEFAULT_PREFIX;
	
	File filenameIn;
	InputStream fileInput;
	String fileOutput;
	XMLSerializer xmlser;
	
	public JAXBSchemaXMLSerializer() {
		try {
			xmlser = new XMLSerializer(JAXB_SCHEMA_PACKAGES);
		} catch (JAXBException e) {
			log.error("impossible to create JAXBContext: "+e);
			log.info("impossible to create JAXBContext", e);
			if(failonerror) { throw new ProcessingException(e); }
		}
	}
	
	@Override
	public void setProperties(Properties prop) {
		fileOutput = prop.getProperty(propertiesPrefix+PROP_XMLSERIALIZATION_JAXB_OUTFILE);
		String fileInputStr = prop.getProperty(propertiesPrefix+PROP_XMLSERIALIZATION_JAXB_INFILE);
		if(fileInputStr==null) {
			fileInputStr = prop.getProperty(propertiesPrefix+PROP_XMLSERIALIZATION_JAXB_INRESOURCE);
			if(fileInputStr!=null) {
				filenameIn = new File(fileInputStr);
				fileInput = JAXBSchemaXMLSerializer.class.getResourceAsStream(fileInputStr);
			}
		}
		else {
			try {
				filenameIn = new File(fileInputStr);
				fileInput = new FileInputStream(new File(fileInputStr));
			} catch (FileNotFoundException e) {
				log.warn("procproperties: File not found: "+fileInputStr);
			}
		}
	}
	
	@Override
	public void dumpSchema(SchemaModel schemaModel) {
		if(fileOutput==null) {
			log.error("xml serialization output file ["+propertiesPrefix+PROP_XMLSERIALIZATION_JAXB_OUTFILE+"] not defined");
			if(failonerror) { throw new ProcessingException("JAXB: xml serialization output file ["+propertiesPrefix+PROP_XMLSERIALIZATION_JAXB_OUTFILE+"] not defined"); }
			return;
		}
		if(schemaModel==null) {
			log.error("schemaModel is null!");
			if(failonerror) { throw new ProcessingException("JAXB: schemaModel is null!"); }
			return;
		}

		try {
			File fout = new File(fileOutput);
			xmlser.marshal(schemaModel, fout);
			log.info("xml schema model dumped to '"+fout.getAbsolutePath()+"'");
		}
		catch(Exception e) {
			log.error("error dumping schema: "+e);
			log.debug("error dumping schema", e);
			if(failonerror) { throw new ProcessingException(e); }
		}
	}

	@Override
	public SchemaModel grabSchema() {
		if(fileInput==null) {
			log.warn("xml serialization input file ["+propertiesPrefix+PROP_XMLSERIALIZATION_JAXB_INFILE+"] not defined");
			return null;
		}

		try {
			SchemaModel sm = (SchemaModel) xmlser.unmarshal(new InputStreamReader(fileInput));
			//use Unmarshaller.afterUnmarshal()?
			validateSchema(sm);
			log.info("xml schema model grabbed from '"+filenameIn.getAbsolutePath()+"'");
			return sm;
		}
		catch(Exception e) {
			log.error("error grabbing schema: "+e);
			log.debug("error grabbing schema", e);
			if(failonerror) { throw new ProcessingException(e); }
			return null;
		}
		
	}
	
	void validateSchema(SchemaModel sm) {
		for(Table t: sm.getTables()) {
			t.validateConstraints();
		}
	}

	@Override
	public Connection getConnection() {
		return null;
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
