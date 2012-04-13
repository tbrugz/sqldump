package tbrugz.sqldump.datadump;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.Writer;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.junit.Test;

import tbrugz.sqldump.datadump.DataDump;
import tbrugz.sqldump.util.IOUtil;

//http://docs.oracle.com/javase/1.4.2/docs/guide/intl/encoding.doc.html
public class DataDumpTest {

	static Log log = LogFactory.getLog(DataDumpTest.class);
	static String DIR = "test/data/";
	static String DIROUT = "work/output/test/";
	
	@Test
	public void testEncoding() throws IOException {
		//DataDump dd = new DataDump();
		Map<String, Writer> map = new HashMap<String, Writer>();
		DataDump.isSetNewFilename(map, DIROUT+"t1-utf8.txt", "UTF-8", null);
		DataDump.isSetNewFilename(map, DIROUT+"t1-iso8859.txt", "ISO-8859-1", null); //ISO8859_1
		for(String s: map.keySet()) {
			map.get(s).write("Pôrto Alégre");
			map.get(s).close();
		}
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
		log.info("f: "+file+"; s: "+s);
		return s;
	}
	
}
