package tbrugz.sqldiff.io;

import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;

import javax.xml.bind.JAXBException;

import org.junit.Assert;
import org.junit.Test;

import tbrugz.sqldiff.model.SchemaDiff;

public class DiffIOTest {

	static String DIR_OUT = "work/output/DiffIOTest";
	
	@Test
	public void testReadWriteXML() throws JAXBException, IOException {
		XMLDiffIO xio = new XMLDiffIO();
		SchemaDiff sd = (SchemaDiff) xio.grabDiff(new InputStreamReader(DiffIOTest.class.getResourceAsStream("diff.xml")));
		Assert.assertEquals(1, sd.getDiffList().size());
		xio.dumpDiff(sd, new File(DIR_OUT+"/diff.xml"));
	}

	@Test
	public void testReadWriteJSON() throws JAXBException, IOException {
		JSONDiffIO jio = new JSONDiffIO();
		SchemaDiff sd = (SchemaDiff) jio.grabDiff(new InputStreamReader(DiffIOTest.class.getResourceAsStream("diff.json")));
		Assert.assertEquals(1, sd.getDiffList().size());
		jio.dumpDiff(sd, new File(DIR_OUT+"/diff.json"));
	}

	@Test
	public void testReadWriteXMLJSON() throws JAXBException, IOException {
		XMLDiffIO xio = new XMLDiffIO();
		JSONDiffIO jio = new JSONDiffIO();
		
		//read xml, write json
		SchemaDiff sd1 = (SchemaDiff) xio.grabDiff(new InputStreamReader(DiffIOTest.class.getResourceAsStream("diff.xml")));
		Assert.assertEquals(1, sd1.getDiffList().size());
		jio.dumpDiff(sd1, new File(DIR_OUT+"/diff.json"));

		//read new json, write xml
		SchemaDiff sd2 = (SchemaDiff) jio.grabDiff(new File(DIR_OUT+"/diff.json"));
		Assert.assertEquals(1, sd2.getDiffList().size());
		xio.dumpDiff(sd2, new File(DIR_OUT+"/diff.xml"));

		//read new xml, write newer json
		SchemaDiff sd3 = (SchemaDiff) xio.grabDiff(new File(DIR_OUT+"/diff.xml"));
		Assert.assertEquals(1, sd3.getDiffList().size());
		jio.dumpDiff(sd3, new File(DIR_OUT+"/diff2.json"));
	}
}
