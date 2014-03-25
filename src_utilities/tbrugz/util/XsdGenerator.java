package tbrugz.util;

import java.io.File;
import java.io.IOException;

import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBException;
import javax.xml.bind.SchemaOutputResolver;
import javax.xml.transform.Result;
import javax.xml.transform.stream.StreamResult;

import tbrugz.sqldump.dbmodel.SchemaModel;

/**
 * see: http://stackoverflow.com/questions/7212064/is-it-possible-to-generate-a-xsd-from-a-jaxb-annotated-class
 * 
 * XXX: add 'targetNamespace' && 'xmlns' attributes?
 */
public class XsdGenerator {

	public class MySchemaOutputResolver extends SchemaOutputResolver {
		public Result createOutput(String namespaceURI, String suggestedFileName) throws IOException {
			File file = new File(suggestedFileName);
			StreamResult result = new StreamResult(file);
			result.setSystemId(file.toURI().toURL().toString());
			System.out.println("file: "+file.getAbsolutePath());
			return result;
		}
	}

	public void generate(Class<?> clazz) throws JAXBException, IOException {
		JAXBContext jaxbContext = JAXBContext.newInstance(clazz);
		SchemaOutputResolver sor = new MySchemaOutputResolver();
		jaxbContext.generateSchema(sor);
	}

	public static void main(String[] args) throws JAXBException, IOException {
		XsdGenerator xsdg = new XsdGenerator();
		xsdg.generate(SchemaModel.class);
	}

}
