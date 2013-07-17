package tbrugz.sqldump.util;

import java.io.IOException;
import java.io.Reader;
import java.io.Writer;

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

public class JSONSerializer extends XMLSerializer {
	
	public JSONSerializer(String contextPath) throws JAXBException {
		super(contextPath);
	}
	
	@Override
	public void marshal(Object object, Writer writer) throws JAXBException {
		Marshaller m = jc.createMarshaller();
		m.setProperty(Marshaller.JAXB_FORMATTED_OUTPUT, true);

		// json-specific
		Configuration config = new Configuration();
		MappedNamespaceConvention con = new MappedNamespaceConvention(config);
		XMLStreamWriter xmlStreamWriter = new MappedXMLStreamWriter(con, writer);
		// /json
		
		m.marshal(object, xmlStreamWriter); //fout
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
		JSONObject obj = new JSONObject(IOUtil.readFile(reader));
		XMLStreamReader xmlStreamReader = new MappedXMLStreamReader(obj, con);
		// /json
		
		Unmarshaller u = jc.createUnmarshaller();
		return u.unmarshal(xmlStreamReader);
	}

}
