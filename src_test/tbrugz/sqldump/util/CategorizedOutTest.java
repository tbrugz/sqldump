package tbrugz.sqldump.util;

import java.io.IOException;

import junit.framework.Assert;

import org.junit.Test;

public class CategorizedOutTest {

	@Test
	public void test1() throws IOException {
		String outpattern = CategorizedOut.generateFinalOutPattern("work/output/test/cout-[ab]-[cd].txt", "ab", "cd");
		CategorizedOut cout = new CategorizedOut(outpattern);
		
		cout.categorizedOut("message1 for x, y", "x", "y");
		cout.categorizedOut("message for 1, 2", "1", "2");
		cout.categorizedOut("message2 for x, y", "x", "y");
		
		String m1 = IOUtil.readFromFilename("work/output/test/cout-1-2.txt");
		Assert.assertEquals("message for 1, 2\n", m1);
		
		String m2 = IOUtil.readFromFilename("work/output/test/cout-x-y.txt");
		Assert.assertEquals("message1 for x, y\nmessage2 for x, y\n", m2);
	}
}
