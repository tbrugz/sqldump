package tbrugz.sqldump.util;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.io.Writer;
import java.util.Arrays;
import java.util.Iterator;
import java.util.Set;
import java.util.TreeSet;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/*
 * TODOne: if filePattern == '<stdout>'|'<stderr>', write to standard out/err streams
 * XXX: pattern for writing to logger? <log:<level>> ? <log:info>, <log.warn>, ? which logger? <log:<logger/class>:<level>>
 * XXX: multiple patterns? Map<String, String>? so it can be used by SchemaModelScriptDumper
 * TODO: add predefined (property) comment to new created files
 * XXX: add method alreadyOpened()/wouldCreateNewFile()?
 */
public class CategorizedOut {
	
	public static class NullWriter extends Writer {
		@Override
		public void write(char[] cbuf, int off, int len) {
		}

		@Override
		public void flush() {
		}

		@Override
		public void close() {
		}
	}
	
	public static class NullOutputStream extends OutputStream {
		@Override
		public void write(int b) throws IOException {
		}
	}
	
	public static interface Callback {
		void callOnOpen(Writer w) throws IOException;
	}
	
	static class NonCloseableWriterDecorator extends Writer {
		final Writer w;
		NonCloseableWriterDecorator(Writer w) {
			this.w = w;
		}
		
		@Override
		public void write(char[] cbuf, int off, int len) throws IOException {
			w.write(cbuf, off, len);
		}
		@Override
		public void flush() throws IOException {
			w.flush();
		}
		@Override
		public void close() throws IOException {
			w.flush();
		}
	}

	static class NonCloseableOutputStreamDecorator extends OutputStream {
		final OutputStream os;
		
		NonCloseableOutputStreamDecorator(OutputStream os) {
			this.os = os;
		}
		
		@Override
		public void write(byte[] b, int off, int len) throws IOException {
			os.write(b, off, len);
		}
		
		@Override
		public void write(byte[] b) throws IOException {
			os.write(b);
		}

		@Override
		public void write(int b) throws IOException {
			os.write(b);
		}
		
		@Override
		public void flush() throws IOException {
			os.flush();
		}
		
		@Override
		public void close() throws IOException {
			os.flush();
		}
		
	}
	
	static class FlushingWriterDecorator extends Writer {
		final Writer w;
		
		public FlushingWriterDecorator(Writer w) {
			this.w = w;
		}

		@Override
		public void write(char[] cbuf, int off, int len) throws IOException {
			w.write(cbuf, off, len);
			w.flush();
		}
		@Override
		public void flush() throws IOException {
			w.flush();
		}
		@Override
		public void close() throws IOException {
			w.close();
		}
	}
	
	class WriterIterator implements Iterator<Writer> {
		final Iterator<String> it;
		WriterIterator() {
			it = filesOpened.iterator();
		}
		
		@Override
		public boolean hasNext() {
			return it.hasNext();
		}

		@Override
		public Writer next() {
			try {
				return getFileWriter(it.next());
			} catch (IOException e) {
				log.warn("IOException: "+e.getMessage());
				return null;
			}
		}

		@Override
		public void remove() {
			it.remove();
			//log.warn("remove: not implemented");
		}
		
	}

	class InnerWriterIterator implements Iterator<Writer> {
		boolean hasNext = true;
		
		@Override
		public boolean hasNext() {
			return hasNext;
		}

		@Override
		public Writer next() {
			hasNext = false;
			return innerWriter;
		}

		@Override
		public void remove() {
		}
	}
	
	class StaticWriterIterator implements Iterator<Writer> {
		boolean hasNext = true;
		final Writer writer;

		StaticWriterIterator(String pattern) {
			try {
				this.writer = getStaticWriter(pattern);
			} catch (IOException e) {
				throw new ExceptionInInitializerError(e);
			}
		}
		
		@Override
		public boolean hasNext() {
			return hasNext;
		}

		@Override
		public Writer next() {
			hasNext = false;
			return writer;
		}

		@Override
		public void remove() {
		}
	}
	
	static final Log log = LogFactory.getLog(CategorizedOut.class);
	
	public static final String STDOUT = "<stdout>"; 
	public static final String STDERR = "<stderr>";
	public static final String NULL_WRITER = "<null>"; // constant for "/dev/null"
	
	static final Writer pwSTDOUT = new NonCloseableWriterDecorator(new PrintWriter(System.out));
	static final Writer pwSTDERR = new NonCloseableWriterDecorator(new PrintWriter(System.err));
	static final Writer nullWriter = new NullWriter();

	static final OutputStream osSTDOUT = new NonCloseableOutputStreamDecorator(System.out);
	static final OutputStream osSTDERR = new NonCloseableOutputStreamDecorator(System.err);
	static final OutputStream nullOS = new NullOutputStream();
	
	public CategorizedOut(String filePathPattern) {
		this(filePathPattern, null);
	}
	
	public CategorizedOut(String filePathPattern, Callback cb) {
		this.filePathPattern = filePathPattern;
		this.innerWriter = null;
		this.cb = cb;
	}

	public CategorizedOut(Writer writer, Callback cb) {
		this.filePathPattern = null;
		this.innerWriter = new NonCloseableWriterDecorator(writer);
		this.cb = cb;
	}
	
	final Set<String> filesOpened = new TreeSet<String>();
	
	//String[] categories = null;
	//XXXdone: final filePathPattern ?
	final String filePathPattern; // XXX: file: /abc/def/${1}file${2}.sql
	
	final NonCloseableWriterDecorator innerWriter;
	boolean innerWriterIgnited = false;
	
	final Callback cb; 
	
	/*public String[] getCategories() {
		return categories;
	}

	public void setCategories(String... categories) {
		this.categories = categories;
	}*/

	public String getFilePathPattern() {
		return filePathPattern;
	}

	/*public void categorizedNewOut(String message, String... categories) throws IOException {
		Writer w = getCategorizedWriter(categories);
		w.write(message+"\n");
		w.flush();
	}*/
	
	//XXXdone: use getCategorizedWriter() (as categorizedNewOut()) ?
	public void categorizedOut(String message, String... categories) throws IOException {
		Writer w = getCategorizedWriter(categories);
		w.write(message+"\n");
		if(innerWriter!=null) {
			w.flush();
		}
		else {
			closeWriter(w);
		}
	}

	public Writer getCategorizedWriter(String... categories) throws IOException {
		if(filePathPattern==null) {
			if(innerWriter!=null) {
				if(!innerWriterIgnited) {
					if(cb!=null) { cb.callOnOpen(innerWriter); }
					innerWriterIgnited = true;
				}
				return innerWriter;
			}
			throw new RuntimeException("filePathPattern not defined, aborting");
		}

		if(isStaticWriter(filePathPattern)) {
			return getStaticWriter(filePathPattern);
		}
		/*if(STDOUT.equals(thisFP)) {
			return pwSTDOUT;
		}
		else if(STDERR.equals(thisFP)) {
			return pwSTDERR;
		}
		else if(NULL_WRITER.equals(thisFP)) {
			return nullWriter;
		}*/
		else {
			String thisFP = getFilePath(categories);
			
			boolean alreadyOpened = filesOpened.contains(thisFP);
			FileWriter fos = getFileWriter(thisFP);
			if(!alreadyOpened && cb!=null) {
				cb.callOnOpen(fos);
			}
			
			return new FlushingWriterDecorator(fos);
		}
	}
	
	public boolean alreadyOpened(String... categories) {
		if(filePathPattern==null) {
			if(innerWriter!=null) {
				return true;
			}
			throw new RuntimeException("filePathPattern not defined, aborting");
		}
		
		String thisFP = getFilePath(categories);
		return filesOpened.contains(thisFP);
	}
	
	//public List<Writer> getAllOpenedWriters() { //XXX: get writer iterator?
	public Iterator<Writer> getAllOpenedWritersIterator() throws IOException {
		if(innerWriter!=null) {
			if(!innerWriterIgnited) {
				if(cb!=null) { cb.callOnOpen(innerWriter); }
				innerWriterIgnited = true;
			}
			return new InnerWriterIterator();
		}
		if(isStaticWriter(filePathPattern)) {
			return new StaticWriterIterator(filePathPattern);
		}
		return new WriterIterator();
	}
	
	String getFilePath(String... categories) {
		String thisFP = new String(filePathPattern);
		
		boolean hasnull = false;
		for(int i=0;i<categories.length;i++) {
			String c = categories[i];
			if(c==null) {
				hasnull = true;
				continue;
			}
			c = Matcher.quoteReplacement(c);
			//String tmpThisFP = thisFP;
			thisFP = thisFP.replaceAll("\\["+(i+1)+"\\]", c);
			//log.debug("fp: "+tmpThisFP+" / "+thisFP);
		}

		if(hasnull) {
			log.debug("cats w/ null: "+Arrays.asList(categories));
		}
		
		return thisFP;
	}
	
	FileWriter getFileWriter(String thisFP) throws IOException {
		File f = new File(thisFP);
		boolean alreadyOpened = filesOpened.contains(thisFP);
		if(!alreadyOpened) {
			filesOpened.add(thisFP);
			log.debug("opening '"+thisFP+"' for writing...");
		
		File dir = f.getParentFile();
		if(dir!=null) {
			
		if(!dir.exists()) {
			if(!dir.mkdirs()) {
				log.warn("error creating dirs: "+dir.getAbsolutePath());
			}
		}
		else {
			if(!dir.isDirectory()) {
				throw new IOException(dir+" already exists and is not a directory");
			}
		}
		
		}
		}
		return new FileWriter(f, alreadyOpened);
	}
	
	@SuppressWarnings("el-syntax")
	public static String generateFinalOutPattern(String outpattern, String... categories) {
		for(int i=0;i<categories.length;i++) {
			String outpatternTmp = outpattern;
			outpattern = outpattern.replaceAll(Pattern.quote(categories[i]), Matcher.quoteReplacement("["+(i+1)+"]"));
			if(!outpatternTmp.equals(outpattern) && categories[i].startsWith("${")) {
				log.warn("using deprecated pattern '${xxx}': "+categories[i]);
			}
		}
		return outpattern;
	}

	@SuppressWarnings("el-syntax")
	public static String generateFinalOutPattern(String outpattern, String[]... categoriesArr) {
		for(int i=0;i<categoriesArr.length;i++) {
			for(int j=0;j<categoriesArr[i].length;j++) {
				String outpatternTmp = outpattern;
				outpattern = outpattern.replaceAll(Pattern.quote(categoriesArr[i][j]), Matcher.quoteReplacement("["+(i+1)+"]"));
				if(!outpatternTmp.equals(outpattern) && categoriesArr[i][j].startsWith("${")) {
					log.warn("using deprecated pattern '${xxx}': "+categoriesArr[i][j]);
				}
			}
		}
		return outpattern;
	}

	public static void closeWriter(Writer w) throws IOException {
		w.flush();
		w.close();
		/*if(pwSTDOUT.equals(w)) {
		}
		else if(pwSTDERR.equals(w)) {
		}
		else if(nullWriter.equals(w)) {
		}
		else {
			w.close();
		}*/
	}

	public static Writer getStaticWriter(String outPattern) throws IOException {
		if(STDOUT.equals(outPattern)) {
			return pwSTDOUT;
		}
		if(STDERR.equals(outPattern)) {
			return pwSTDERR;
		}
		if(NULL_WRITER.equals(outPattern)) {
			return nullWriter;
		}
		return null;
	}
	
	public static boolean isStaticWriter(String outPattern) {
		if(STDOUT.equals(outPattern)) {
			return true;
		}
		if(STDERR.equals(outPattern)) {
			return true;
		}
		if(NULL_WRITER.equals(outPattern)) {
			return true;
		}
		return false;
	}
	
	public static OutputStream getStaticOutputStream(String outPattern) throws IOException {
		if(STDOUT.equals(outPattern)) {
			return osSTDOUT;
		}
		if(STDERR.equals(outPattern)) {
			return osSTDERR;
		}
		if(NULL_WRITER.equals(outPattern)) {
			return nullOS;
		}
		return null;
	}
	
	public static boolean isStaticOutputStream(String outPattern) throws IOException {
		if(STDOUT.equals(outPattern)) {
			return true;
		}
		if(STDERR.equals(outPattern)) {
			return true;
		}
		if(NULL_WRITER.equals(outPattern)) {
			return true;
		}
		return false;
	}

}
