package tbrugz.sqldump.datadump;

import java.io.IOException;
import java.util.List;

import org.junit.Assert;
import org.junit.Test;

public class DumpSyntaxRegistryTest {

	@Test
	public void testGetSyntaxes() throws IOException {
		Class<?>[] someClasses = new Class<?>[]{InsertIntoDataDump.class, CSVDataDump.class, XMLDataDump.class};
		{
			List<Class<DumpSyntax>> cls = DumpSyntaxRegistry.getSyntaxes();
			//System.out.println(cls);
			for(Class<?> c: someClasses) {
				Assert.assertTrue(cls.contains(c));
			}
		}
		{
			DumpSyntaxRegistry.setSyntaxesResource("/dumpsyntaxes-test.properties");
			List<Class<DumpSyntax>> clsTest = DumpSyntaxRegistry.getSyntaxes();
			Assert.assertEquals(2, clsTest.size());
			//System.out.println(clsTest);
		}
		{
			DumpSyntaxRegistry.setDefultSyntaxesResource();
			List<Class<DumpSyntax>> cls = DumpSyntaxRegistry.getSyntaxes();
			//System.out.println(cls);
			for(Class<?> c: someClasses) {
				Assert.assertTrue(cls.contains(c));
			}
		}
	}
	
}
