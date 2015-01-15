package tbrugz.sqldump.util;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
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
	
	public static interface Callback {
		void callOnOpen(Writer w) throws IOException;
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
	
	static final Log log = LogFactory.getLog(CategorizedOut.class);
	
	public static final String STDOUT = "<stdout>"; 
	public static final String STDERR = "<stderr>";
	public static final String NULL_WRITER = "<null>"; // constant for "/dev/null"
	
	static final Writer pwSTDOUT = new PrintWriter(System.out);
	static final Writer pwSTDERR = new PrintWriter(System.err);
	static final Writer nullWriter = new NullWriter();
	
	public CategorizedOut(String filePathPattern) {
		this(filePathPattern, null);
	}
	
	public CategorizedOut(String filePathPattern, Callback cb) {
		this.filePathPattern = filePathPattern;
		this.cb = cb;
	}

	final Set<String> filesOpened = new TreeSet<String>();
	
	//String[] categories = null;
	//XXXdone: final filePathPattern ?
	final String filePathPattern; // XXX: file: /abc/def/${1}file${2}.sql
	
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

	public void categorizedNewOut(String message, String... categories) throws IOException {
		Writer w = getCategorizedWriter(categories);
		w.write(message+"\n");
		w.flush();
	}
	
	//XXXdone: use getCategorizedWriter() (as categorizedNewOut()) ?
	public void categorizedOut(String message, String... categories) throws IOException {
		Writer w = getCategorizedWriter(categories);
		w.write(message+"\n");
		closeWriter(w);
	}

	public Writer getCategorizedWriter(String... categories) throws IOException {
		if(filePathPattern==null) {
			throw new RuntimeException("filePathPattern not defined, aborting");
		}

		String thisFP = getFilePath(categories);
		
		if(STDOUT.equals(thisFP)) {
			return pwSTDOUT;
		}
		else if(STDERR.equals(thisFP)) {
			return pwSTDERR;
		}
		else if(NULL_WRITER.equals(thisFP)) {
			return nullWriter;
		}
		else {
			boolean alreadyOpened = filesOpened.contains(thisFP);
			FileWriter fos = getFileWriter(thisFP);
			if(!alreadyOpened && cb!=null) {
				cb.callOnOpen(fos);
			}
			
			return fos;
		}
	}
	
	public boolean alreadyOpened(String... categories) {
		if(filePathPattern==null) {
			throw new RuntimeException("filePathPattern not defined, aborting");
		}
		
		String thisFP = getFilePath(categories);
		return filesOpened.contains(thisFP);
	}
	
	//public List<Writer> getAllOpenedWriters() { //XXX: get writer iterator?
	public Iterator<Writer> getAllOpenedWritersIterator() {
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
		if(pwSTDOUT.equals(w)) {
		}
		else if(pwSTDERR.equals(w)) {
		}
		else if(nullWriter.equals(w)) {
		}
		else {
			w.close();
		}
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
	
}
