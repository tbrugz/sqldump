package tbrugz.sqldump.util;

import java.io.FileInputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.Reader;
import java.io.StringWriter;
import java.io.Writer;
import java.nio.ByteBuffer;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class IOUtil {
	
	static final int BUFFER_SIZE = 1024*8;
	
	static final Log log = LogFactory.getLog(IOUtil.class);
	
	static void writeFile(String contents, Writer writer) throws IOException {
		writer.write(contents);
	}

	public static String readFile(Reader reader) throws IOException {
		return readFromReader(reader);
	}
	
	public static String readFromReader(Reader reader) throws IOException {
		StringWriter sw = new StringWriter();
		
		char[] cbuf = new char[BUFFER_SIZE];
		
		int iread = reader.read(cbuf);
		
		while(iread>0) {
			sw.write(cbuf, 0, iread);
			iread = reader.read(cbuf);
		}

		return sw.toString();
	}

	public static ByteBuffer readFileBytes(String filename) throws IOException {
		return readFileBytes(new FileInputStream(filename));
	}
	
	public static ByteBuffer readFileBytes(InputStream is) throws IOException {
		ByteBuffer bb = ByteBuffer.allocate(BUFFER_SIZE*2);
		
		byte[] buf = new byte[BUFFER_SIZE];
		
		int iread = is.read(buf);
		
		while(iread>0) {
			bb.put(buf);
			iread = is.read(buf);
		}
		
		return bb;
	}
	
	/*public static String readFromFilename(File fileName) {
		return readFromFilename(fileName.getAbsolutePath());
	}*/
	
	public static String readFromFilename(String fileName) {
		try {
			Reader reader = new FileReader(fileName);
			String ret = IOUtil.readFromReader(reader);
			reader.close();
			return ret;
		} catch (IOException e) {
			log.warn("error reading file "+fileName+": "+e);
		}
		return null;
	}
	
	public static void pipeStreams(InputStream is, OutputStream os) throws IOException {
		byte[] buffer = new byte[BUFFER_SIZE];
		int len;
		while ((len = is.read(buffer)) != -1) {
			os.write(buffer, 0, len);
		}
	}

	public static void pipeCharacterStreams(Reader r, Writer w) throws IOException {
		char[] buffer = new char[BUFFER_SIZE];
		int len;
		while ((len = r.read(buffer)) != -1) {
			w.write(buffer, 0, len);
		}
	}
	
	public static String readFromResource(String resourcePath) {
		return readFromResource(resourcePath, Thread.currentThread().getContextClassLoader());
	}
	
	public static String readFromResource(String resourcePath, ClassLoader classloader) {
		InputStream is = classloader.getResourceAsStream(resourcePath);
		return readFromInputStream(is, resourcePath);
	}
	
	public static String readFromInputStream(InputStream is, String resourcePath) {
		if(is==null) {
			log.warn("can't read resource: "+resourcePath);
			return null;
		}
		
		try {
			String ret = IOUtil.readFromReader(new InputStreamReader(is));
			is.close();
			return ret;
		} catch (IOException e) {
			log.warn("error reading resource "+resourcePath+": "+e);
		}
		
		return null;
	}

}
