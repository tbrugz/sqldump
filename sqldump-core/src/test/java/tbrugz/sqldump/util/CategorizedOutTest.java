package tbrugz.sqldump.util;

import java.io.IOException;

import org.junit.Assert;
import org.junit.Test;

//XXX: test static writers, inner writers, getCategorizedWriter
public class CategorizedOutTest {

	@Test
	public void testSimplePatternAndOutput() throws IOException {
		String outpattern = CategorizedOut.generateFinalOutPattern("target/work/output/test/cout-[ab]-[cd].txt",
				"[ab]", "[cd]");
		Assert.assertEquals("target/work/output/test/cout-[1]-[2].txt", outpattern);
		
		CategorizedOut cout = new CategorizedOut(outpattern);
		
		cout.categorizedOut("message1 for x, y", "x", "y");
		cout.categorizedOut("message for 1, 2", "1", "2");
		cout.categorizedOut("message2 for x, y", "x", "y");
		
		String m1 = IOUtil.readFromFilename("target/work/output/test/cout-1-2.txt");
		Assert.assertEquals("message for 1, 2\n", m1);
		
		String m2 = IOUtil.readFromFilename("target/work/output/test/cout-x-y.txt");
		Assert.assertEquals("message1 for x, y\nmessage2 for x, y\n", m2);
	}

	@Test
	public void testArrayPattern() throws IOException {
		String outpattern = CategorizedOut.generateFinalOutPattern("target/work/output/test/cout-[ab]-[cd].txt",
				new String[]{"[ab]", "[ac]"}, new String[]{"[ca]", "[cd]"});
		Assert.assertEquals("target/work/output/test/cout-[1]-[2].txt", outpattern);
	}

	//XXX: remove deprecated patterns
	@Test
	public void testDeprecatedPattern() throws IOException {
		String outpattern = CategorizedOut.generateFinalOutPattern("target/work/output/test/cout-${ab}.txt",
				"${ab}");
		//System.out.println("out: "+outpattern);
		Assert.assertEquals("target/work/output/test/cout-[1].txt", outpattern);

		String outpatternArr = CategorizedOut.generateFinalOutPattern("target/work/output/test/cout-${abc}.txt",
				new String[]{"${q}", "${w}"},
				new String[]{"${abc}", "${123}"});
		Assert.assertEquals("target/work/output/test/cout-[2].txt", outpatternArr);
	}
}
