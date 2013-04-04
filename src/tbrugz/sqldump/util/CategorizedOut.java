package tbrugz.sqldump.util;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.Writer;
import java.util.Arrays;
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
		public void write(char[] cbuf, int off, int len) throws IOException {
		}

		@Override
		public void flush() throws IOException {
		}

		@Override
		public void close() throws IOException {
		}
	}
	
	static final Log log = LogFactory.getLog(CategorizedOut.class);
	
	public static final String STDOUT = "<stdout>"; 
	public static final String STDERR = "<stderr>";
	public static final String NULL_WRITER = "<null>"; // constant for "/dev/null"
	
	final static Writer pwSTDOUT = new PrintWriter(System.out);
	final static Writer pwSTDERR = new PrintWriter(System.err);
	final static Writer nullWriter = new NullWriter();
	
	public CategorizedOut(String filePathPattern) {
		this.filePathPattern = filePathPattern;
	}

	@Deprecated
	public CategorizedOut() {
		this(null);
	}
	
	Set<String> filesOpened = new TreeSet<String>();
	
	//String[] categories = null;
	//XXX: final filePathPattern ?
	String filePathPattern; // XXX: file: /abc/def/${1}file${2}.sql
	
	/*public String[] getCategories() {
		return categories;
	}

	public void setCategories(String... categories) {
		this.categories = categories;
	}*/

	public String getFilePathPattern() {
		return filePathPattern;
	}

	//XXX @Deprecated ?
	public void setFilePathPattern(String filePathPattern) {
		this.filePathPattern = filePathPattern;
	}

	public void categorizedNewOut(String message, String... categories) throws IOException {
		Writer w = getCategorizedWriter(categories);
		w.write(message+"\n");
		w.flush();
	}
	
	//XXX: use getCategorizedWriter() (as categorizedNewOut()) ?
	public void categorizedOut(String message, String... categories) throws IOException {
		if(filePathPattern==null) {
			throw new RuntimeException("filePathPattern not defined, aborting");
		}

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
			log.debug("cats w/ null: "+Arrays.asList(categories)+" ; message = "+message);
		}
		
		if(STDOUT.equals(thisFP)) {
			System.out.println(message);
		}
		else if(STDERR.equals(thisFP)) {
			System.err.println(message);
		}
		else if(NULL_WRITER.equals(thisFP)) {
			nullWriter.write(message); //not realy needed...
		}
		else {
			boolean alreadyOpened = filesOpened.contains(thisFP);
			if(!alreadyOpened) { filesOpened.add(thisFP); }
			
			File f = new File(thisFP);
			//String dirStr = f.getParent();
			File dir = new File(f.getParent());
			if(!dir.exists()) {
				dir.mkdirs();
			}
			else {
				if(!dir.isDirectory()) {
					throw new IOException(dir+" already exists and is not a directory");
				}
			}
			FileWriter fos = new FileWriter(f, alreadyOpened); //if already opened, append; if not, create
			//XXX: remove '\n'?
			fos.write(message+"\n");
			fos.close();
		}
	}

	public Writer getCategorizedWriter(String... categories) throws IOException {
		if(filePathPattern==null) {
			throw new RuntimeException("filePathPattern not defined, aborting");
		}

		String thisFP = new String(filePathPattern);
		
		boolean hasnull = false;
		for(int i=0;i<categories.length;i++) {
			String c = categories[i];
			if(c==null) {
				hasnull = true;
				continue;
			}
			c = Matcher.quoteReplacement(c);
			thisFP = thisFP.replaceAll("\\["+(i+1)+"\\]", c);
		}
		
		if(hasnull) {
			log.debug("cats w/ null: "+Arrays.asList(categories));
		}
		
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
			if(!alreadyOpened) { filesOpened.add(thisFP); }
			
			File f = new File(thisFP);
			//String dirStr = f.getParent();
			File dir = new File(f.getParent());
			if(!dir.exists()) {
				dir.mkdirs();
			}
			else {
				if(!dir.isDirectory()) {
					throw new IOException(dir+" already exists and is not a directory");
				}
			}
			FileWriter fos = new FileWriter(f, alreadyOpened); //if already opened, append; if not, create
			return fos;
		}
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

}
