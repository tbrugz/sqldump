package tbrugz.sqldump;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.Reader;
import java.io.StringWriter;
import java.io.Writer;

public class IOUtil {
	
	public static int BUFFER_SIZE = 1024*8;
	
	static void writeFile(String contents, Writer writer) throws IOException {
		writer.write(contents);
	}

	public static String readFile(Reader reader) throws IOException {
		StringWriter sw = new StringWriter();
		
		char[] cbuf = new char[BUFFER_SIZE];
		
		int iread = reader.read(cbuf);
		
		while(iread>0) {
			sw.write(cbuf, 0, iread);
			iread = reader.read(cbuf);
		}

		return sw.toString();
	}
	
	public static void pipeStreams(InputStream is, OutputStream os) throws IOException {
		byte[] buffer = new byte[BUFFER_SIZE];
		int len;
		while ((len = is.read(buffer)) != -1) {
		    os.write(buffer, 0, len);
		}
	}
}
