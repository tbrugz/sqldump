package tbrugz.sqldump.util;

import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.Reader;
import java.io.Writer;

import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBException;
import javax.xml.bind.Marshaller;
import javax.xml.bind.Unmarshaller;

public class XMLSerializer {

	JAXBContext jc;
	
	public XMLSerializer(String contextPath) throws JAXBException {
		jc = JAXBContext.newInstance( contextPath );
	}
	
	public Object unmarshal(Reader reader) throws JAXBException {
		Unmarshaller u = jc.createUnmarshaller();
		Object o = u.unmarshal(reader);
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
		m.marshal(object, writer);
	}
	
	public void marshal(Object object, File file) throws JAXBException, IOException {
		Writer w = new FileWriter(file);
		marshal(object, w);
		w.close();
	}
}
