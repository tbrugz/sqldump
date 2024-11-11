package tbrugz.sqldump.util;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.Reader;
import java.io.StringWriter;
import java.io.Writer;
import java.net.URL;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class IOUtil {
	
	static final int BUFFER_SIZE = 1024*8;
	
	static final Log log = LogFactory.getLog(IOUtil.class);
	
	static void writeFile(String contents, Writer writer) throws IOException {
		writer.write(contents);
	}

	@Deprecated
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
		return readFromFile(new File(fileName));
	}

	public static String readFromFile(File file) {
		try {
			Reader reader = new FileReader(file);
			String ret = IOUtil.readFromReader(reader);
			reader.close();
			return ret;
		} catch (IOException e) {
			log.warn("error reading file "+file+": "+e);
		}
		return null;
	}

	public static String readFromFile(File file, String inputEncoding) {
		// https://stackoverflow.com/questions/696626/java-filereader-encoding-issue
		try {
			Reader reader = new InputStreamReader(new FileInputStream(file), inputEncoding);
			String ret = IOUtil.readFromReader(reader);
			reader.close();
			return ret;
		} catch (IOException e) {
			log.warn("error reading file "+file+" [with encoding "+inputEncoding+"]: "+e);
		}
		return null;
	}

	public static String readFromFile(File file, String inputEncoding, boolean throwException) throws IOException {
		// https://stackoverflow.com/questions/696626/java-filereader-encoding-issue
		try {
			if(inputEncoding==null) {
				inputEncoding = Charset.defaultCharset().name();
			}
			Reader reader = new InputStreamReader(new FileInputStream(file), inputEncoding);
			String ret = IOUtil.readFromReader(reader);
			reader.close();
			return ret;
		} catch (IOException e) {
			log.warn("error reading file "+file+" [with encoding "+inputEncoding+"]: "+e);
			if(throwException) {
				throw e;
			}
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

	public static void pipeStreams(InputStream is, OutputStream os, long maxBytes) throws IOException {
		byte[] buffer = new byte[BUFFER_SIZE];
		long bytesRead = 0;
		int len;
		while ((len = is.read(buffer)) != -1) {
			bytesRead += len;
			if(bytesRead>maxBytes) {
				len -= bytesRead-maxBytes;
				os.write(buffer, 0, len);
				break;
			}
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
	
	public static void pipeCharacterStreams(Reader r, Writer w, long maxChars) throws IOException {
		char[] buffer = new char[BUFFER_SIZE];
		long bytesRead = 0;
		int len;
		while ((len = r.read(buffer)) != -1) {
			bytesRead += len;
			if(bytesRead>maxChars) {
				len -= bytesRead-maxChars;
				w.write(buffer, 0, len);
				break;
			}
			w.write(buffer, 0, len);
		}
	}
	
	public static String readFromResource(String resourcePath) {
		InputStream is = getResourceAsStream(resourcePath);
		return readFromInputStream(is, resourcePath);
		//return readFromResource(resourcePath, Thread.currentThread().getContextClassLoader());
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

	// see: https://github.com/quarkusio/quarkus/issues/2531
	public static URL getResource(String path) {
		URL url = IOUtil.class.getResource(path);
		if(url==null) {
			ClassLoader classloader = Thread.currentThread().getContextClassLoader();
			url = classloader.getResource(path);
		}
		return url;
	}

	// see: https://github.com/quarkusio/quarkus/issues/2531
	public static InputStream getResourceAsStream(String path) {
		InputStream is = IOUtil.class.getResourceAsStream(path);
		if(is==null) {
			ClassLoader classloader = Thread.currentThread().getContextClassLoader();
			is = classloader.getResourceAsStream(path);
		}
		return is;
	}
	
	/*
	// see: Utils.prepareDir()
	public static boolean createDirIfNotExists(File dir) {
		if(!dir.exists()) {
			boolean created = dir.mkdirs();
			log.debug("creating dir: "+dir+" ["+created+"]");
			return created;
		}
		return false;
	}
	*/

}
