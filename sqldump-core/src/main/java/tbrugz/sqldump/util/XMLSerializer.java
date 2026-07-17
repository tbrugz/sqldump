package tbrugz.sqldump.util;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.Reader;
import java.io.Writer;

import javax.xml.stream.XMLInputFactory;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamReader;

import jakarta.xml.bind.JAXBContext;
import jakarta.xml.bind.JAXBException;
import jakarta.xml.bind.Marshaller;
import jakarta.xml.bind.Unmarshaller;

public class XMLSerializer {

	final JAXBContext jc;
	final XMLInputFactory xif;
	
	public XMLSerializer(String contextPath) throws JAXBException {
		this( JAXBContext.newInstance(contextPath, Thread.currentThread().getContextClassLoader()) );
	}

	public XMLSerializer(JAXBContext jc) throws JAXBException {
		this.jc = jc;
		xif = XMLInputFactory.newFactory();
		xif.setProperty(XMLInputFactory.SUPPORT_DTD, false);
		xif.setProperty(XMLInputFactory.IS_SUPPORTING_EXTERNAL_ENTITIES, false);
	}
	
	public Object unmarshal(Reader reader) throws JAXBException {
		/*
		Unmarshaller u = jc.createUnmarshaller();
		Object o = u.unmarshal(reader);
		*/
		// https://cheatsheetseries.owasp.org/cheatsheets/XML_External_Entity_Prevention_Cheat_Sheet.html#jaxb-unmarshaller
		XMLStreamReader xsr = null;
		try {
			xsr = xif.createXMLStreamReader(reader);
		} catch (XMLStreamException e) {
			throw new RuntimeException(e);
		}
		Unmarshaller um = jc.createUnmarshaller();
		Object o = um.unmarshal(xsr);
		//use Unmarshaller.afterUnmarshal()?
		return o;
	}

	public Object unmarshal(File file) throws JAXBException, IOException {
		FileReader fread = new FileReader(file);
		Object ret = unmarshal(fread);
		fread.close();
		return ret;
	}
	
	public void marshal(Object object, Writer writer) throws JAXBException {
		Marshaller m = jc.createMarshaller();
		//XXX: property for formatting or not JAXB output?
		//see: http://ws.apache.org/jaxme/release-0.3/manual/ch02s02.html
		m.setProperty(Marshaller.JAXB_FORMATTED_OUTPUT, true);
		//m.setProperty(Marshaller.JAXB_NO_NAMESPACE_SCHEMA_LOCATION, "http://tbrugz.bitbucket.org/sqldump");
		//m.setProperty(Marshaller.JAXB_SCHEMA_LOCATION, "http://tbrugz.bitbucket.org/sqldump");
		m.marshal(object, writer);
	}
	
	/*
	public void marshal(Object object, File file) throws JAXBException, IOException {
		Writer w = new FileWriter(file);
		marshal(object, w);
		w.close();
	}
	*/
	
}
