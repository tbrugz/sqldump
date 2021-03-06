package tbrugz.sqldump.util;

import java.io.IOException;
import java.io.Reader;
import java.io.StringWriter;
import java.io.Writer;

import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBException;
import javax.xml.bind.Marshaller;
import javax.xml.bind.Unmarshaller;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamReader;
import javax.xml.stream.XMLStreamWriter;

import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;
import org.codehaus.jettison.mapped.Configuration;
import org.codehaus.jettison.mapped.MappedNamespaceConvention;
import org.codehaus.jettison.mapped.MappedXMLStreamReader;
import org.codehaus.jettison.mapped.MappedXMLStreamWriter;
import org.codehaus.jettison.util.StringIndenter;

public class JSONSerializer extends XMLSerializer {
	
	boolean indent = true;
	
	public JSONSerializer(String contextPath) throws JAXBException {
		super(contextPath);
	}
	
	public JSONSerializer(JAXBContext jc) throws JAXBException {
		super(jc);
	}
	
	@Override
	public void marshal(Object object, Writer writer) throws JAXBException {
		Marshaller m = jc.createMarshaller();
		//m.setProperty(Marshaller.JAXB_FORMATTED_OUTPUT, true);
		
		Writer sw = writer;
		if(indent) {
			sw = new StringWriter();
		}

		// json-specific
		Configuration config = new Configuration();
		MappedNamespaceConvention con = new MappedNamespaceConvention(config);
		XMLStreamWriter xmlStreamWriter = new MappedXMLStreamWriter(con, sw);
		// /json
		
		m.marshal(object, xmlStreamWriter); //fout
		
		if(!indent) {
			return;
		}
		
		//see http://jira.codehaus.org/browse/JETTISON-43
		StringIndenter si = new StringIndenter(sw.toString());
		String result = si.result();
		
		try {
			writer.write(result);
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}
	
	@Override
	public Object unmarshal(Reader reader) throws JAXBException {
		try {
			return unmarshalInternal(reader);
		} catch (JSONException e) {
			throw new RuntimeException(e);
		} catch (IOException e) {
			throw new RuntimeException(e);
		} catch (XMLStreamException e) {
			throw new RuntimeException(e);
		}
	}
	
	Object unmarshalInternal(Reader reader) throws JAXBException, JSONException, IOException, XMLStreamException {
		// json-specific
		Configuration config = new Configuration();
		MappedNamespaceConvention con = new MappedNamespaceConvention(config);
		JSONObject obj = new JSONObject(IOUtil.readFromReader(reader));
		XMLStreamReader xmlStreamReader = new MappedXMLStreamReader(obj, con);
		// /json
		
		Unmarshaller u = jc.createUnmarshaller();
		return u.unmarshal(xmlStreamReader);
	}

}
