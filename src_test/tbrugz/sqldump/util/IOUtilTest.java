package tbrugz.sqldump.util;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.Reader;
import java.io.UnsupportedEncodingException;
import java.io.Writer;

import org.junit.Assert;
import org.junit.Test;

public class IOUtilTest {

	static String DIR = "test/data/";
	
	@Test
	public void readFromFilename() {
		String sread = IOUtil.readFromFilename("src_test/tbrugz/sqldump/util/p1.properties");
		Assert.assertEquals("id1=value1", sread);
	}

	@Test
	public void writeAndRead() throws IOException {
		String file = "work/output/util-ioutiltest.txt";
		Utils.prepareDir(new File(file));
		Writer w = new FileWriter(file);
		IOUtil.writeFile("abc123", w);
		w.close();
		Reader r = new FileReader(file);
		String sread = IOUtil.readFromReader(r);
		r.close();
		Assert.assertEquals("abc123", sread);
	}
	
	@Test
	public void testReadFile() throws FileNotFoundException {
		readFile("t1-ansi.txt");
		readFile("t1-utf8.txt");
		readFile("t1-utf8bom.txt");
		readFile("t2-ansi.txt");
		readFile("t2-utf8.txt");
	}

	public String readFile(String file) throws FileNotFoundException {
		String s = IOUtil.readFromFilename(DIR+file);
		//log.info("f: "+file+"; s: "+s);
		return s;
	}
	
	@Test
	public void testParseBase64() throws UnsupportedEncodingException {
		String s = Utils.parseBase64("YWJjZA==");
		Assert.assertEquals("abcd", s);
	}

	@Test
	public void testPrintBase64() {
		String s = Utils.printBase64("abcd");
		System.out.println("s: "+s);
		Assert.assertEquals("YWJjZA==", s);
	}
	
}
