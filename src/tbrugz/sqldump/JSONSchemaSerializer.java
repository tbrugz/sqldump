package tbrugz.sqldump;

import java.io.File;
import java.io.InputStream;
import java.io.InputStreamReader;

import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import tbrugz.sqldump.dbmodel.SchemaModel;
import tbrugz.sqldump.def.ProcessingException;
import tbrugz.sqldump.def.SchemaModelDumper;
import tbrugz.sqldump.def.SchemaModelGrabber;
import tbrugz.sqldump.util.JSONSerializer;

/*
 * see: http://blog.bdoughan.com/2011/04/jaxb-and-json-via-jettison.html
 */
public class JSONSchemaSerializer extends JAXBSchemaXMLSerializer implements SchemaModelDumper, SchemaModelGrabber {

	static final Log log = LogFactory.getLog(JSONSchemaSerializer.class);
	
	public static final String JSONSERIALIZATION_DEFAULT_PREFIX = "sqldump.jsonserialization";
	
	JSONSerializer jsonser; //XXX: final?

	public JSONSchemaSerializer() {
		propertiesPrefix = JSONSERIALIZATION_DEFAULT_PREFIX;
		try {
			JAXBContext jc = JAXBContext.newInstance(JAXB_SCHEMA_PACKAGES);
			jsonser = new JSONSerializer(jc);
		} catch (JAXBException e) {
			log.error("impossible to create JAXBContext: "+e);
			log.info("impossible to create JAXBContext", e);
			if(failonerror) { throw new ProcessingException(e); }
		}
	}
	
	@Override
	public SchemaModel grabSchema() {
		InputStream is = getInputStream();
		if(is==null) {
			log.warn("json serialization input file ["+propertiesPrefix+PROP_XMLSERIALIZATION_JAXB_INFILE+"] nor resource ["+propertiesPrefix+PROP_XMLSERIALIZATION_JAXB_INRESOURCE+"] are valid");
			return null;
		}

		try {
			SchemaModel sm = (SchemaModel) jsonser.unmarshal(new InputStreamReader(is));
			is.close();
			//use Unmarshaller.afterUnmarshal()?
			validateSchema(sm);
			log.info("json schema model grabbed from '"+inputDescription+"'");
			return sm;
		}
		catch(Exception e) {
			log.error("error grabbing schema: "+e);
			log.debug("error grabbing schema", e);
			if(failonerror) { throw new ProcessingException(e); }
			return null;
		}
	}

	@Override
	public void dumpSchema(SchemaModel schemaModel) {
		if(outputWriter==null) {
			log.error("json serialization output ["+propertiesPrefix+PROP_XMLSERIALIZATION_JAXB_OUTFILE+"] not defined");
			if(failonerror) { throw new ProcessingException("JAXB: json serialization output ["+propertiesPrefix+PROP_XMLSERIALIZATION_JAXB_OUTFILE+"] not defined"); }
			return;
		}
		if(schemaModel==null) {
			log.error("schemaModel is null!");
			if(failonerror) { throw new ProcessingException("JAXB/JSON: schemaModel is null!"); }
			return;
		}

		try {
			jsonser.marshal(schemaModel, outputWriter);
			outputWriter.flush();
			if(fileOutput!=null) {
				File fout = new File(fileOutput);
				log.info("json schema model dumped to '"+fout.getAbsolutePath()+"'");
			}
			else {
				log.info("json schema model dumped to output-stream");
			}
		}
		catch(Exception e) {
			log.error("error dumping schema: "+e);
			log.debug("error dumping schema", e);
			if(failonerror) { throw new ProcessingException(e); }
		}
	}
	
	@Override
	public String getMimeType() {
		return "application/json";
	}

}
