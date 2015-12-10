package tbrugz.sqldump;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Writer;
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
	
	String filenameIn;
	String resourceIn;
	String inputDescription;
	String fileOutput;
	Writer outputWriter;
	XMLSerializer xmlser;
	String grabberId;
	
	public JAXBSchemaXMLSerializer() {
		try {
			xmlser = new XMLSerializer(JAXB_SCHEMA_PACKAGES);
		} catch (JAXBException e) {
			log.error(getIdDesc()+"impossible to create JAXBContext: "+e);
			log.info("impossible to create JAXBContext", e);
			if(failonerror) { throw new ProcessingException(e); }
		}
	}
	
	@Override
	public void setProperties(Properties prop) {
		fileOutput = prop.getProperty(propertiesPrefix+PROP_XMLSERIALIZATION_JAXB_OUTFILE);
		try {
			if(fileOutput!=null) {
				outputWriter = new FileWriter(fileOutput);
			}
			/*else {
				log.warn("xml serialization output file ["+propertiesPrefix+PROP_XMLSERIALIZATION_JAXB_OUTFILE+"] not defined");
			}*/
		} catch (IOException e) {
			log.warn(e);
		}
		filenameIn = prop.getProperty(propertiesPrefix+PROP_XMLSERIALIZATION_JAXB_INFILE);
		resourceIn = prop.getProperty(propertiesPrefix+PROP_XMLSERIALIZATION_JAXB_INRESOURCE);
	}
	
	@Override
	public void dumpSchema(SchemaModel schemaModel) {
		if(outputWriter==null) {
			log.error("xml serialization output ["+propertiesPrefix+PROP_XMLSERIALIZATION_JAXB_OUTFILE+"] not defined");
			if(failonerror) { throw new ProcessingException("JAXB: xml serialization output ["+propertiesPrefix+PROP_XMLSERIALIZATION_JAXB_OUTFILE+"] not defined"); }
			return;
		}
		if(schemaModel==null) {
			log.error("schemaModel is null!");
			if(failonerror) { throw new ProcessingException("JAXB: schemaModel is null!"); }
			return;
		}

		try {
			xmlser.marshal(schemaModel, outputWriter);
			outputWriter.flush();
			if(fileOutput!=null) {
				File fout = new File(fileOutput);
				log.info(getIdDesc()+"xml schema model dumped to '"+fout.getAbsolutePath()+"'");
			}
			else {
				log.info(getIdDesc()+"xml schema model dumped to output-stream");
			}
		}
		catch(Exception e) {
			log.error(getIdDesc()+"error dumping schema: "+e);
			log.debug("error dumping schema", e);
			if(failonerror) { throw new ProcessingException(e); }
		}
	}

	@Override
	public SchemaModel grabSchema() {
		InputStream is = getInputStream();
		if(is==null) {
			log.warn("xml serialization input file ["
					+propertiesPrefix+PROP_XMLSERIALIZATION_JAXB_INFILE+"] nor resource ["
					+propertiesPrefix+PROP_XMLSERIALIZATION_JAXB_INRESOURCE+"] are valid [prefix="+propertiesPrefix+"]");
			return null;
		}

		try {
			SchemaModel sm = (SchemaModel) xmlser.unmarshal(new InputStreamReader(is));
			is.close();
			//use Unmarshaller.afterUnmarshal()?
			validateSchema(sm);
			log.info(getIdDesc()+"xml schema model grabbed from '"+inputDescription+"'");
			return sm;
		}
		catch(Exception e) {
			log.error("error grabbing schema: "+e);
			log.debug("error grabbing schema", e);
			if(failonerror) { throw new ProcessingException(e); }
			return null;
		}
		
	}
	
	InputStream getInputStream() {
		InputStream is = null;
		if(filenameIn!=null) {
			try {
				is = new FileInputStream(filenameIn);
				inputDescription = new File(filenameIn).getAbsolutePath();
			} catch (FileNotFoundException e) {
				String message = "file not found: "+filenameIn;
				log.warn(message);
				if(failonerror) { throw new ProcessingException(message, e); }
			}
		}
		else if(resourceIn!=null) {
			is = JAXBSchemaXMLSerializer.class.getResourceAsStream(resourceIn);
			inputDescription = resourceIn;
			if(is==null) {
				String message = "resource not found: "+resourceIn;
				log.warn(message);
				if(failonerror) { throw new ProcessingException(message); }
			}
		}
		else {
			String message = "props '"+PROP_XMLSERIALIZATION_JAXB_INFILE+"' nor '"+PROP_XMLSERIALIZATION_JAXB_INRESOURCE+"' defined";
			log.warn(message);
			if(failonerror) { throw new ProcessingException(message); }
		}
		
		return is;
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
	
	@Override
	public boolean acceptsOutputWriter() {
		return true;
	}
	
	@Override
	public void setOutputWriter(Writer writer) {
		outputWriter = writer;
	}
	
	@Override
	public String getMimeType() {
		return "application/xml";
	}
	
	@Override
	public void setId(String grabberId) {
		this.grabberId = grabberId;
	}
	
	String getIdDesc() {
		return grabberId!=null?"["+grabberId+"] ":"";
	}

}
