package tbrugz.sqldump.util;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class ParametrizedProperties extends Properties {

	private static final long serialVersionUID = 1L;
	public static final String DIRECTIVE_INCLUDE = "@includes";
	static Log log = LogFactory.getLog(ParametrizedProperties.class);

	static boolean useSystemProperties = false;

	//List<File> loadedPropFiles = new ArrayList<File>();
	Map<File, Boolean> loadedPropFiles = new HashMap<File, Boolean>(); //boolean is: hasWarned
	
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
					InputStream is = ParametrizedProperties.class.getResourceAsStream(f);
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
						log.warn("error loading @include '"+f+"': "+e.getMessage());
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
		log.debug("getp: "+key);
		String s = null;
		if(useSystemProperties) {
			s = System.getProperty(key);
		}
		if(s==null) {
			s = super.getProperty(key);
		}
		
		if(s==null) {
			return null;
		}
		if(s.indexOf("${")<0) {
			return s;
		}
		
		return replaceProps(s, this);
	}

	public static String replaceProps(String s, Properties p) {
		if(s==null) { return null; }
		StringBuffer sb = new StringBuffer(s);
		//s.replaceAll("\\$\\{(.*?)\\}", );
		int count = 0;
		int pos1;
		while((pos1 = sb.indexOf("${", count)) >= 0) {
			int pos2 = sb.indexOf("}", pos1);
			if(pos2<0) { break; }
			count = pos1+1;
			String prop = sb.substring(pos1+2, pos2);
			String propSuperValue = p.getProperty(prop);
			
			if(useSystemProperties && propSuperValue==null) {
				propSuperValue = System.getProperty(prop);
			}
			
			if(propSuperValue==null) { continue; }
			
			sb.replace(pos1, pos2+1, propSuperValue);
		}
		return sb.toString();
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
	
}
