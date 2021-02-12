package tbrugz.sqldump.util;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.Properties;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class CLIProcessor {

	public static final String PROJECT_URL = "https://github.com/tbrugz/sqldump";

	//static/constant properties
	public static final String PROP_PROPFILEBASEDIR = "propfilebasedir"; //"propfiledir" / "propfilebasedir" / "propertiesbasedir" / "basepropdir"
	
	//cli parameters
	protected static final String PARAM_HELP_DEFAULT = "--help";

	protected static final String PARAM_PROPERTIES_FILENAME = "-propfile=";
	protected static final String PARAM_PROPERTIES_RESOURCE = "-propresource=";
	protected static final String PREFIX_DEFINE_PROPERTY = "-D";
	protected static final String PARAM_USE_SYSPROPERTIES = "-usesysprop=";
	protected static final String[] PARAMS_VERSION = {"-v", "--version"};
	protected static final String[] PARAMS_HELP = {"-?", "-h", PARAM_HELP_DEFAULT};
	
	static final Log log = LogFactory.getLog(CLIProcessor.class);
	
	//XXX: move to utils(?)... (used by sqldump & sqlrun -- why not sqldiff?) 
	public static void init(final String productName, final String[] args,
			final String defaultPropFile, final Properties papp) throws IOException {
		log.info((productName!=null?productName+" ":"")+
				"init... [version "+Version.getVersion()+"]");
		log.debug("buildNumber: "+Version.getBuildNumber()+" ; buildTimestamp: "+Version.getBuildTimestamp());
		boolean useSysPropSetted = false;
		int loadedCount = 0;
		if(args!=null) {
			for(String arg: args) {
				if(arg.indexOf(PARAM_PROPERTIES_FILENAME)==0) {
					String propFilename = arg.substring(PARAM_PROPERTIES_FILENAME.length());
					boolean loaded = loadFile(papp, propFilename);
					if(loaded) { loadedCount++; }
				}
				else if(arg.indexOf(PARAM_PROPERTIES_RESOURCE)==0) {
					String propResource = arg.substring(PARAM_PROPERTIES_RESOURCE.length());
					boolean loaded = loadResource(papp, propResource);
					if(loaded) { loadedCount++; }
				}
				else if(arg.indexOf(PREFIX_DEFINE_PROPERTY)==0) {
					String propMap = arg.substring(PREFIX_DEFINE_PROPERTY.length());
					int idx = propMap.indexOf("=");
					if(idx>=0) {
						String propName = propMap.substring(0, idx);
						String propValue = propMap.substring(idx+1);
						log.debug("setting property '"+propName+"' with value '"+propValue+"'");
						papp.setProperty(propName, propValue);
					}
					else {
						log.warn("invalid value setting property, arg: "+arg);
						//throw new IllegalArgumentException("Invalid property syntax: "+arg);
					}
				}
				else if(arg.indexOf(PARAM_USE_SYSPROPERTIES)==0) {
					String useSysProp = arg.substring(PARAM_USE_SYSPROPERTIES.length());
					ParametrizedProperties.setUseSystemProperties(useSysProp.equalsIgnoreCase("true"));
					useSysPropSetted = true;
				}
				else {
					String message = "Unrecognized param '"+arg+"' (ignored). for more information use '"+PARAM_HELP_DEFAULT+"' argument";
					log.error(message);
					System.out.println(message);
					throw new IllegalArgumentException(message);
					//System.exit(1);
					//show help and exit?
					//System.out.println(getHelpText(productName));
				}
			}
		}
		if(loadedCount==0) {
			boolean loaded = loadFile(papp, defaultPropFile, true);
			if(loaded) { loadedCount++; }
		}

		//log.debug(papp.size()+" properties defined ; "+loadedCount+" files or resources read");
		if(papp.size()==0 || (papp.containsKey(PROP_PROPFILEBASEDIR) && papp.size()==1)) {
			log.warn("no properties loaded"+(loadedCount==0?" (and no properties file or resource read)":""));
			//XXX throw exception?
		}
		if(!useSysPropSetted) {
			ParametrizedProperties.setUseSystemProperties(true); //set to true by default
			useSysPropSetted = true;
		}
		log.debug("using sys properties: "+ParametrizedProperties.isUseSystemProperties());
	}
	
	public static boolean shouldStopExec(final String productName, final String[] args) {
		if(args!=null) {
			for(String arg: args) {
				if(StringUtils.contains(PARAMS_VERSION, arg)) {
					System.out.println((productName!=null?productName+" ":"")+
						Version.getVersion()+
						" ("+Version.getBuildNumber()+")");
					return true;
				}
				else if(StringUtils.contains(PARAMS_HELP, arg)) {
					System.out.println(getHelpText(productName));
					return true;
				}
			}
		}
		return false;
	}

	static String getHelpText(final String productName) {
		final int pad = 28;
		return (productName!=null?productName+" ":"")+Version.getVersion()+"\n\n"
			+ "Usage: sqldump [options]\n\n"
			+ "Options:\n\n"
			+ "  "+StringUtils.rightPad(PARAM_PROPERTIES_FILENAME+"<file>", pad)+" use <file> properties\n"
			+ "  "+StringUtils.rightPad(PARAM_PROPERTIES_RESOURCE+"<resource>", pad)+" use <resource> properties\n"
			+ "  "+StringUtils.rightPad(PREFIX_DEFINE_PROPERTY+"<property>=<value>", pad)+" define property <property> with value <value>\n"
			+ "  "+StringUtils.rightPad(PARAM_USE_SYSPROPERTIES+"[true|false]", pad)+" use system properties (default is true)\n"
			+ "  "+StringUtils.rightPad(Utils.join(Arrays.asList(PARAMS_HELP),", "), pad)+" show help and exit\n"
			+ "  "+StringUtils.rightPad(Utils.join(Arrays.asList(PARAMS_VERSION),", "), pad)+" show version and exit\n"
			+ "\nMore info at <"+PROJECT_URL+">";
	}
	
	static boolean loadResource(Properties p, String propResource) {
		log.info("loading properties resource: "+propResource);
		InputStream propIS = CLIProcessor.class.getResourceAsStream(propResource);
		if(propIS==null) {
			log.warn("properties resource '"+propResource+"' does not exist");
			return false;
		}
		try {
			p.load(propIS);
			propIS.close();
			return true;
		}
		catch(IOException e) {
			log.warn("error loading resource '"+propResource+"': "+e);
			return false;
		}
	}
	
	static boolean loadFile(Properties p, String propFilename) {
		return loadFile(p, propFilename, false);
	}
	
	static boolean loadFile(Properties p, String propFilename, boolean defaultFile) {
		File propFile = new File(propFilename);
		File propFileDir = propFile.getAbsoluteFile().getParentFile();
		log.debug("propfile base dir: "+propFileDir);
		p.setProperty(PROP_PROPFILEBASEDIR, propFileDir.toString());
		
		try {
			InputStream propIS = new FileInputStream(propFile);
			p.load(propIS);
			propIS.close();
			log.info("loaded "+(defaultFile?"default ":"")+"properties file: "+propFile);
			return true;
		}
		catch(FileNotFoundException e) {
			String message = (defaultFile?"default ":"")+"properties file '"+propFile+"' not found: "+e.getMessage();
			if(defaultFile) {
				log.info(message);
			}
			else {
				log.warn(message);
			}
			return false;
		}
		catch(IOException e) {
			log.warn("error loading "+(defaultFile?"default ":"")+"file '"+propFile+"': "+e);
			return false;
		}
	}

}
