package tbrugz.sqldump.datadump;

import java.io.IOException;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import tbrugz.sqldump.util.IOUtil;
import tbrugz.sqldump.util.Utils;

public class DumpSyntaxRegistry {
	
	static final Log log = LogFactory.getLog(DumpSyntaxRegistry.class);
	
	static final String SYNTAXES_PROPERTIES = "/dumpsyntaxes.properties";
	static final String PROP_CLASSES = "dumpsyntax.classes";
	
	static String syntaxesProperties = SYNTAXES_PROPERTIES;

	static final List<Class<DumpSyntax>> syntaxes = new ArrayList<Class<DumpSyntax>>();
	static boolean initted = false;
	
	static void init() throws IOException {
		Properties prop = new Properties();
		prop.load(IOUtil.getResourceAsStream(syntaxesProperties));
		List<String> classes = Utils.getStringListFromProp(prop, PROP_CLASSES, ",");
		loadClasses(classes);
		initted = true;
	}
	
	static void loadClasses(List<String> classes) {
		for(String c: classes) {
			loadClass(c);
		}
	}
	
	@SuppressWarnings("unchecked")
	static boolean loadClass(String c) {
		Class<?> cc = Utils.getClassWithinPackages(c, "tbrugz.sqldump.datadump", null);
		if(cc==null) {
			log.warn("dump syntax class '"+c+"' could not be loaded");
		}
		else if (DumpSyntax.class.isAssignableFrom(cc)) {
			if(syntaxes.contains(cc)) {
				log.debug("dump syntaxes already contains "+cc.getName());
			}
			else {
				syntaxes.add((Class<DumpSyntax>)cc);
				return true;
			}
		}
		else {
			log.warn("class '"+c+"' is not a subclass of DumpSyntax");
		}
		return false;
	}
	
	public static void addSyntaxes(String classes) {
		if(classes==null) { return; }
		if(!initted) {
			try {
				init();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		String[] ss = classes.split(",");
		for(String s: ss) {
			s = s.trim();
			if(loadClass(s)) {
				log.info("xtra syntax '"+s+"' loaded");
			}
		}
	}
	
	public static List<Class<DumpSyntax>> getSyntaxes() {
		if(!initted) {
			try {
				init();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		return syntaxes;
	}
	
	public static void setSyntaxesResource(String syntaxesResource) throws IOException {
		URL u = IOUtil.getResource(syntaxesResource);
		if(u!=null) {
			syntaxesProperties = syntaxesResource;
			clearSyntaxes();
			log.debug("syntaxes resource set to '"+syntaxesResource+"'");
		}
		else {
			log.warn("syntaxes resource '"+syntaxesResource+"' not found");
		}
	}
	
	public static void setDefultSyntaxesResource() throws IOException {
		setSyntaxesResource(SYNTAXES_PROPERTIES);
	}
	
	static void clearSyntaxes() {
		syntaxes.clear();
		initted = false;
	}

}
