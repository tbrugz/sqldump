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

	/*
	 *  http://stackoverflow.com/questions/740751/how-do-you-link-xml-to-a-xsd
	 *  http://stackoverflow.com/questions/16979762/adding-namespaces-to-root-element-of-xml-using-jaxb
	 *  http://stackoverflow.com/questions/3451615/how-do-i-set-the-xml-namespace-when-using-jersey-jaxb-jax-rs
	 *  https://blogs.oracle.com/enterprisetechtips/entry/customizing_jaxb
	 *  http://stackoverflow.com/questions/4312572/jaxb-generated-xml-problem-with-root-element-prefix
	 *  http://stackoverflow.com/questions/6680804/how-to-debug-marshaling-in-jaxb
	 *  
	 *  https://jaxb.java.net/guide/Customizing_Java_packages.html
	 *  
	 *  
	 *  http://docs.oracle.com/javase/7/docs/api/javax/xml/bind/JAXBContext.html
	 *  com.sun.xml.internal.bind.v2.ContextFactory
	 */
	
	static String schemaFileName = "target/sqld-model.xsd"; // schemamodel.xsd ?

	public class MySchemaOutputResolver extends SchemaOutputResolver {
		@Override
		public Result createOutput(String namespaceURI, String suggestedFileName) throws IOException {
			System.out.println("namespaceURI: "+namespaceURI+" ; suggestedFileName: "+suggestedFileName);
			File file = new File(schemaFileName);
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
