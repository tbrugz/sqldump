package tbrugz.sqldump;

import java.io.File;
import java.io.FileWriter;
import java.io.InputStreamReader;

import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBException;
import javax.xml.bind.Marshaller;
import javax.xml.bind.Unmarshaller;
import javax.xml.stream.XMLStreamReader;
import javax.xml.stream.XMLStreamWriter;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.codehaus.jettison.json.JSONObject;
import org.codehaus.jettison.mapped.Configuration;
import org.codehaus.jettison.mapped.MappedNamespaceConvention;
import org.codehaus.jettison.mapped.MappedXMLStreamReader;
import org.codehaus.jettison.mapped.MappedXMLStreamWriter;

import tbrugz.sqldump.dbmodel.SchemaModel;
import tbrugz.sqldump.dbmodel.Table;
import tbrugz.sqldump.def.ProcessingException;
import tbrugz.sqldump.def.SchemaModelDumper;
import tbrugz.sqldump.def.SchemaModelGrabber;
import tbrugz.sqldump.util.IOUtil;

/*
 * see: http://blog.bdoughan.com/2011/04/jaxb-and-json-via-jettison.html
 */
public class JSONSchemaSerializer extends JAXBSchemaXMLSerializer implements SchemaModelDumper, SchemaModelGrabber {

	static final Log log = LogFactory.getLog(JSONSchemaSerializer.class);
	
	public static final String JSONSERIALIZATION_DEFAULT_PREFIX = "sqldump.jsonserialization";

	public JSONSchemaSerializer() {
		propertiesPrefix = JSONSERIALIZATION_DEFAULT_PREFIX;
		try {
			jc = JAXBContext.newInstance( "tbrugz.sqldump.dbmodel:tbrugz.sqldump.dbmsfeatures" );
		} catch (JAXBException e) {
			log.error("impossible to create JAXBContext: "+e);
			log.info("impossible to create JAXBContext", e);
			if(failonerror) { throw new ProcessingException(e); }
		}
	}
	
	@Override
	public SchemaModel grabSchema() {
		if(fileInput==null) {
			log.warn("json serialization input file ["+propertiesPrefix+PROP_XMLSERIALIZATION_JAXB_INFILE+"] not defined");
			return null;
		}

		try {
			Unmarshaller u = jc.createUnmarshaller();
			
			Configuration config = new Configuration();
			MappedNamespaceConvention con = new MappedNamespaceConvention(config);
			JSONObject obj = new JSONObject(IOUtil.readFile(new InputStreamReader(fileInput)));
			XMLStreamReader xmlStreamReader = new MappedXMLStreamReader(obj, con);
			
			SchemaModel sm = (SchemaModel) u.unmarshal(xmlStreamReader);
			//use Unmarshaller.afterUnmarshal()?
			for(Table t: sm.getTables()) {
				t.validateConstraints();
			}
			log.info("json schema model grabbed from '"+filenameIn.getAbsolutePath()+"'");
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
		if(fileOutput==null) {
			log.error("json serialization output file ["+propertiesPrefix+PROP_XMLSERIALIZATION_JAXB_OUTFILE+"] not defined");
			if(failonerror) { throw new ProcessingException("JAXB: json serialization output file ["+propertiesPrefix+PROP_XMLSERIALIZATION_JAXB_OUTFILE+"] not defined"); }
			return;
		}
		if(schemaModel==null) {
			log.error("schemaModel is null!");
			if(failonerror) { throw new ProcessingException("JAXB/JSON: schemaModel is null!"); }
			return;
		}

		try {
			Configuration config = new Configuration();
			MappedNamespaceConvention con = new MappedNamespaceConvention(config);
			
			Marshaller m = jc.createMarshaller();
			/*
			 * XXX: how to format JSON output? String formatted = new StringIndenter(jsonString).result(); ?
			 * see:
			 * - Human readable JSON output option - http://jira.codehaus.org/browse/JETTISON-43
			 * - http://svn.codehaus.org/jettison/trunk/src/main/java/org/codehaus/jettison/util/StringIndenter.java
			 */
			File fout = new File(fileOutput);
			FileWriter fw = new FileWriter(fout);
			XMLStreamWriter xmlStreamWriter = new MappedXMLStreamWriter(con, fw);
			m.marshal(schemaModel, xmlStreamWriter);
			log.info("xml schema model dumped to '"+fout.getAbsolutePath()+"'");
		}
		catch(Exception e) {
			log.error("error dumping schema: "+e);
			log.debug("error dumping schema", e);
			if(failonerror) { throw new ProcessingException(e); }
		}
	}

}
