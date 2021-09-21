package tbrugz.sqldump.util;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/*
 * http://docs.oracle.com/javaee/5/tutorial/doc/bnahq.html - Java Unified Expression Language
 * http://books.sonatype.com/mvnref-book/reference/resource-filtering-sect-properties.html - Maven Properties
 */
public class ParametrizedProperties extends Properties {

	private static final long serialVersionUID = 1L;

	public static final String DIRECTIVE_INCLUDE = "@includes";
	public static final String NULL_PLACEHOLDER = "_NULL_";

	static final Log log = LogFactory.getLog(ParametrizedProperties.class);

	static boolean useSystemProperties = false;
	static boolean useSystemEnvironment = true;
	static boolean nullValueReturnsNull = true;

	//List<File> loadedPropFiles = new ArrayList<File>();
	Map<File, Boolean> loadedPropFiles = new HashMap<File, Boolean>(); //boolean is: hasWarned
	
	//TODO: process @includes at 'end'
	
	/*
	 * system properties that may be used in @import
	 * 
	 * other sys properties that might be used, maybe...
	 * java.home
	 * java.class.path
	 * java.library.path
	 * java.io.tmpdir
	 * java.ext.dirs
	 * user.name
	 * 
	 * see: http://docs.oracle.com/javase/1.5.0/docs/api/java/lang/System.html#getProperties%28%29
	 */
	static final String SYSPROPKEY_USER_HOME = "user.home";
	static final String SYSPROPKEY_USER_DIR = "user.dir";

	static final String ENVPROPKEY_PREPEND = "env.";
	
	static final String PROPFILEBASEDIR_PATTERN = Pattern.quote("${"+CLIProcessor.PROP_PROPFILEBASEDIR+"}");
	static final String SYS_USER_HOME_PROP = Pattern.quote("${"+SYSPROPKEY_USER_HOME+"}");
	static final String SYS_USER_HOME_VALUE = Matcher.quoteReplacement(""+System.getProperty(SYSPROPKEY_USER_HOME));
	static final String SYS_USER_DIR_PROP = Pattern.quote("${"+SYSPROPKEY_USER_DIR+"}");
	static final String SYS_USER_DIR_VALUE = Matcher.quoteReplacement(""+System.getProperty(SYSPROPKEY_USER_DIR));
	
	@Override
	public synchronized void load(final InputStream inStream) throws IOException {
		//TODOne: load in temp Properties; load from @include directive; load from temp Properties
		Properties ptmp = new Properties();
		if(inStream==null) {
			throw new IOException("input stream could not be read");
		}
		ptmp.load(inStream); //should be in the beggining so that getProperty(DIRECTIVE_INCLUDE) works
		
		String includes = ptmp.getProperty(DIRECTIVE_INCLUDE);
		if(includes!=null) {
			String baseDir = getProperty(CLIProcessor.PROP_PROPFILEBASEDIR);
			if(baseDir!=null) {
				includes = includes.replaceAll(PROPFILEBASEDIR_PATTERN, Matcher.quoteReplacement(baseDir));
			}
			includes = includes.replaceAll(SYS_USER_HOME_PROP, SYS_USER_HOME_VALUE);
			includes = includes.replaceAll(SYS_USER_DIR_PROP, SYS_USER_DIR_VALUE);
			String[] files = includes.split(",");
			for(String f: files) {
				f = f.trim();
				File ff = new File(f);
				if(loadedPropFiles.containsKey(ff)) {
					if(loadedPropFiles.get(ff)) {
						log.warn("already loaded prop file: "+ff.getAbsolutePath());
					}
					loadedPropFiles.put(ff, true);
					continue;
				}
				loadedPropFiles.put(ff, false);
				try {
					InputStream is = IOUtil.getResourceAsStream(f);
					log.debug("loading @include resource: "+f);
					this.load(is);
					log.info("loaded @include resource: "+f);
				} catch (IOException e) {
					try {
						log.debug("loading @include: "+ff.getAbsolutePath());
						this.load(new FileInputStream(ff));
						log.info("loaded @include: "+ff.getCanonicalPath());
					}
					catch(IOException e2) {
						log.warn("error loading @include '"+f+"': "+e.getMessage()+" [user.dir='"+System.getProperty(SYSPROPKEY_USER_DIR)+"']");
						log.debug("error loading @include: "+f, e);
					}
				}
			}
		}
		
		super.putAll(ptmp); //'main' props are prefered (loaded at end)!
	}
	
	@Override
	public String getProperty(String key, String defaultValue) {
		String val = getProperty(key);
		return (val == null) ? defaultValue : val;
	}
	
	@Override
	public String getProperty(String key) {
		return getProperty(key, true);
	}

	String getProperty(String key, boolean replaceNullPlaceholder) {
		if(log.isDebugEnabled()) { logKey(key); }
		
		String s = null;
		// precedence: system props, env vars, (file) properties
		if(useSystemProperties) {
			s = System.getProperty(key);
		}
		if(useSystemEnvironment && key.startsWith(ENVPROPKEY_PREPEND) && s==null) {
			//log.info("getenv '"+key+"': "+System.getenv(key.substring(ENVPROPKEY_PREPEND.length())));
			s = System.getenv(key.substring(ENVPROPKEY_PREPEND.length()));
		}
		if(s==null) {
			s = super.getProperty(key);
		}
		
		if(s==null) {
			return null;
		}
		if(s.indexOf("${")<0) {
			if(replaceNullPlaceholder && nullValueReturnsNull && s.equals(NULL_PLACEHOLDER)) {
				return null;
			}
			return s;
		}

		s = replaceProps(s);
		
		if(replaceNullPlaceholder && nullValueReturnsNull && s.equals(NULL_PLACEHOLDER)) {
			return null;
		}
		return s;
	}
	
	@Override
	public synchronized void clear() {
		super.clear();
		loadedPropFiles.clear();
	}

	String replaceProps(String s) {
		if(s==null) { return null; }
		StringBuilder sb = new StringBuilder(s);
		//s.replaceAll("\\$\\{(.*?)\\}", );
		int count = 0;
		int pos1;
		while((pos1 = sb.indexOf("${", count)) >= 0) {
			int pos2 = sb.indexOf("}", pos1);
			if(pos2<0) { break; }
			count = pos1+1;
			String prop = sb.substring(pos1+2, pos2);
			String propSuperValue = null;
			if(prop.contains("|")) {
				String[] parts = prop.split("\\|");
				String propval = null;
				for(int i=0;i<parts.length;i++) {
					String propKey = parts[i].trim();
					/*if(nullValueReturnsNull && propKey.equals(NULL_PLACEHOLDER)) {
						return null;
					}*/
					propval = getProperty(propKey, false); // recursive call...
					if(propval!=null) { break; }
				}
				propSuperValue = propval;
			}
			else {
				propSuperValue = getProperty(prop.trim(), false);
			}
			
			if(useSystemProperties && propSuperValue==null) {
				propSuperValue = System.getProperty(prop.trim());
			}
			
			if(propSuperValue==null) { continue; }
			
			sb.replace(pos1, pos2+1, propSuperValue);
		}
		String ret = sb.toString();
		return ret;
	}

	public static String replaceProps(String s, Properties p) {
		if(s==null) { return null; }
		StringBuilder sb = new StringBuilder(s);
		//s.replaceAll("\\$\\{(.*?)\\}", );
		int count = 0;
		int pos1;
		while((pos1 = sb.indexOf("${", count)) >= 0) {
			int pos2 = sb.indexOf("}", pos1);
			if(pos2<0) { break; }
			count = pos1+1;
			String prop = sb.substring(pos1+2, pos2);
			String propSuperValue = null;
			if(prop.contains("|")) {
				String[] parts = prop.split("\\|");
				String propval = null;
				for(int i=0;i<parts.length;i++) {
					String propKey = parts[i].trim();
					/*if(nullValueReturnsNull && propKey.equals(NULL_PLACEHOLDER)) {
						return null;
					}*/
					propval = p.getProperty(propKey); // recursive call...
					if(propval!=null) { break; }
				}
				propSuperValue = propval;
			}
			else {
				propSuperValue = p.getProperty(prop.trim());
			}
			
			if(useSystemProperties && propSuperValue==null) {
				propSuperValue = System.getProperty(prop.trim());
			}
			
			if(propSuperValue==null) { continue; }
			
			sb.replace(pos1, pos2+1, propSuperValue);
		}
		String ret = sb.toString();
		return ret;
	}
	
	//XXX: make non-static?
	public static boolean isUseSystemProperties() {
		return useSystemProperties;
	}

	//XXX: make non-static? add useSystemPropertiesParam to constructor?
	public static void setUseSystemProperties(boolean useSystemPropertiesParam) {
		log.debug("using system properties: "+useSystemPropertiesParam);
		useSystemProperties = useSystemPropertiesParam;
	}

	public static boolean isNullValueReturnsNull() {
		return nullValueReturnsNull;
	}

	public static void setNullValueReturnsNull(boolean nullValueReturnsNullParam) {
		nullValueReturnsNull = nullValueReturnsNullParam;
	}
	
	void logKey(String key) {
		if(key.startsWith("sqldump.")) {}
		else if(key.startsWith("sqldiff.")) {}
		else if(key.startsWith("sqlrun.")) {}
		else if(key.startsWith("dbid.")) {}
		else if(key.startsWith("type.")) {}
		else if(key.startsWith("dbids")) {}
		else if(key.startsWith("column.")) {}
		else if(key.startsWith("outputdir")) {}
		else if(key.startsWith("propfilebasedir")) {}
		else if(key.startsWith("dbms.")) {}
		/*
		else if(key.startsWith("")) {}
		else if(key.startsWith("")) {}
		else if(key.startsWith("")) {}
		else if(key.startsWith("")) {}
		else if(key.startsWith("")) {}
		*/
		else {
			log.debug("get: "+key);
		}
	}
	
}
