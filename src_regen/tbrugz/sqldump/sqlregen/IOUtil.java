package tbrugz.sqldump.sqlregen;

import java.io.IOException;
import java.io.Reader;
import java.io.StringWriter;
import java.io.Writer;

public class IOUtil {
	static void writeFile(String contents, Writer writer) throws IOException {
		writer.write(contents);
	}

	static String readFile(Reader reader) throws IOException {
		StringWriter sw = new StringWriter();
		
		char[] cbuf = new char[4096];
		
		int iread = reader.read(cbuf);
		
		while(iread>0) {
			sw.write(cbuf, 0, iread);
			iread = reader.read(cbuf);
		}

		return sw.toString();
	}
}
