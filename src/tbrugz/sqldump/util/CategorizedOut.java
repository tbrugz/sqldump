package tbrugz.sqldump.util;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Set;
import java.util.TreeSet;
import java.util.regex.Matcher;

/*
 * TODOne: if filePattern == '<stdout>'|'<stderr>', write to standard out/err streams
 * XXX: multiple patterns? Map<String, String>? so it can be used by SchemaModelScriptDumper
 */
public class CategorizedOut {
	
	public static final String STDOUT = "<stdout>"; 
	public static final String STDERR = "<stderr>";
	
	Set<String> filesOpened = new TreeSet<String>();
	
	//String[] categories = null;
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

	public void setFilePathPattern(String filePathPattern) {
		this.filePathPattern = filePathPattern;
	}

	public void categorizedOut(String message, String... categories) throws IOException {
		if(filePathPattern==null) {
			throw new RuntimeException("filePathPattern not defined, aborting");
		}

		String thisFP = new String(filePathPattern);
		
		for(int i=0;i<categories.length;i++) {
			String c = categories[i];
			c = Matcher.quoteReplacement(c);
			//c = c.replaceAll("\\$", "\\\\\\$"); //indeed strange but necessary if objectName contains "$". see Matcher.replaceAll() & Matcher.quoteReplacement()
			thisFP = thisFP.replaceAll("\\$\\{"+(i+1)+"\\}", c);
		}
		
		if(STDOUT.equals(thisFP)) {
			System.out.println(message);
		}
		else if(STDERR.equals(thisFP)) {
			System.err.println(message);
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
	
	public static void main(String[] args) throws IOException {
		CategorizedOut co = new CategorizedOut();
		co.setFilePathPattern("d:/temp/abc_${1}_${2}_${1}.txt");
		co.categorizedOut("hello", "a", "$123");
		//System.out.println(Matcher.quoteReplacement("\\$123"));
	}

}
