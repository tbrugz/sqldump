package tbrugz.sqldump.util;

import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.Reader;
import java.io.Writer;

import junit.framework.Assert;

import org.junit.Test;

public class IOUtilTest {

	@Test
	public void readFromFilename() {
		String sread = IOUtil.readFromFilename("src_test/tbrugz/sqldump/util/p1.properties");
		Assert.assertEquals("id1=value1", sread);
	}

	@Test
	public void writeAndRead() throws IOException {
		String file = "work/output/util-ioutiltest.txt";
		Writer w = new FileWriter(file);
		IOUtil.writeFile("abc123", w);
		w.close();
		Reader r = new FileReader(file);
		String sread = IOUtil.readFile(r);
		r.close();
		Assert.assertEquals("abc123", sread);
	}
}
