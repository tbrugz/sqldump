package tbrugz.sqldump.datadump;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import tbrugz.sqldump.util.Utils;

public class DumpSyntaxRegistry {
	static final Log log = LogFactory.getLog(DumpSyntaxRegistry.class);
	
	static final String SYNTAXES_PROPERTIES = "/dumpsyntaxes.properties";
	static final String PROP_CLASSES = "dumpsyntax.classes";

	static final List<Class<? extends DumpSyntax>> syntaxes = new ArrayList<Class<? extends DumpSyntax>>();
	static boolean initted = false;
	
	static void init() throws IOException {
		Properties prop = new Properties();
		prop.load(DumpSyntaxRegistry.class.getResourceAsStream(SYNTAXES_PROPERTIES));
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
	static void loadClass(String c) {
		Class<?> cc = Utils.getClassWithinPackages(c, "tbrugz.sqldump.datadump", null);
		if(cc==null) {
			log.warn("dum syntax class '"+c+"' was not found");
		}
		else if (DumpSyntax.class.isAssignableFrom(cc)) {
			syntaxes.add((Class<? extends DumpSyntax>)cc);
		}
		else {
			log.warn("class '"+c+"' is not a subclass of DumpSyntax");
		}
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
			loadClass(s);
			log.info("xtra syntax '"+s+"' loaded");
		}
	}
	
	public static List<Class<? extends DumpSyntax>> getSyntaxes() {
		if(!initted) {
			try {
				init();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		return syntaxes;
	}
	
	//XXX: add clearSyntaxes() ?

}
