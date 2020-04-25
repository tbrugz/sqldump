package tbrugz.sqldump.util;

import java.io.IOException;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class ParametrizedPropertiesTest {
	
	public static final String path = "/tbrugz/sqldump/util/";
	
	ParametrizedProperties pp = null;
	
	@Before
	public void setup() {
		pp = new ParametrizedProperties();
		ParametrizedProperties.setUseSystemProperties(false);
	}
	
	@Test
	public void testP1() throws IOException {
		pp.load(ParametrizedProperties.class.getResourceAsStream(path+"p1.properties"));
		Assert.assertEquals("value1", pp.getProperty("id1"));
	}

	@Test
	public void testP2Include() throws IOException {
		pp.load(ParametrizedProperties.class.getResourceAsStream(path+"p2.properties"));
		Assert.assertEquals("value1", pp.getProperty("id1"));
	}

	@Test
	public void testP3() throws IOException {
		pp.load(ParametrizedProperties.class.getResourceAsStream(path+"p3.properties"));
		Assert.assertEquals("value2", pp.getProperty("id1"));
	}

	@Test
	public void testP3SystemProperties() throws IOException {
		ParametrizedProperties.setUseSystemProperties(true);
		System.setProperty("id1", "system");
		pp.load(ParametrizedProperties.class.getResourceAsStream(path+"p3.properties"));
		Assert.assertEquals("system", pp.getProperty("id1"));
		Assert.assertEquals("id2.value", pp.getProperty("id2"));
	}

	@Test
	public void testP4IncludeWithBaseDir() throws IOException {
		pp.setProperty(CLIProcessor.PROP_PROPFILEBASEDIR, path);
		pp.load(ParametrizedProperties.class.getResourceAsStream(path+"p4.properties"));
		Assert.assertEquals("value1", pp.getProperty("id1"));
	}

	@Test
	public void testP4GetEnv() throws IOException {
		pp.setProperty(CLIProcessor.PROP_PROPFILEBASEDIR, path);
		pp.load(ParametrizedProperties.class.getResourceAsStream(path+"p4.properties"));
		Assert.assertTrue("JAVA_HOME env should be set: "+pp.getProperty("envtest"), !pp.getProperty("envtest").startsWith("${"));
	}

	@Test
	public void testP4Coalesce() throws IOException {
		pp.setProperty(CLIProcessor.PROP_PROPFILEBASEDIR, path);
		pp.load(ParametrizedProperties.class.getResourceAsStream(path+"p4.properties"));
		Assert.assertEquals("aaa", pp.getProperty("prop3"));
		Assert.assertEquals("aaa", pp.getProperty("prop4"));
		Assert.assertEquals("bbb", pp.getProperty("prop5"));
		
		//ParametrizedProperties.setUseSystemProperties(true);
		//System.out.println("propDir: "+pp.getProperty("propDir"));
	}

	@Test
	public void testP4NullPlaceholder() throws IOException {
		pp.setProperty(CLIProcessor.PROP_PROPFILEBASEDIR, path);
		pp.load(ParametrizedProperties.class.getResourceAsStream(path+"p4.properties"));

		Assert.assertEquals(null, pp.getProperty("null"));
		Assert.assertEquals(null, pp.getProperty("propNull1"));
		Assert.assertEquals(null, pp.getProperty("propNull2"));
		Assert.assertEquals("something _NULL_ and something", pp.getProperty("propNull3"));
		Assert.assertEquals(null, pp.getProperty("p2prop"));
	}

}
