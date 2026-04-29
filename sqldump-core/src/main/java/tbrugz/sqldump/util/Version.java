package tbrugz.sqldump.util;

import java.io.IOException;
import java.util.Properties;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class Version {

	private static final Log log = LogFactory.getLog(Version.class);

	static final String PROP_VERSION = "project.version";
	static final String PROP_BUILD_NUMBER = "build.revisionNumber";
	static final String PROP_BUILD_TIMESTAMP = "build.timestamp";
	
	static final Properties prop = new Properties();
	
	static {
		try {
			prop.load(IOUtil.getResourceAsStream("/sqldump-version.properties"));
		} catch (IOException e) {
			//e.printStackTrace();
			log.warn("IOException: "+e);
		}
	}
	
	public static String getVersion() {
		return prop.getProperty(PROP_VERSION);
	}

	public static String getBuildNumber() {
		return prop.getProperty(PROP_BUILD_NUMBER);
	}
	
	public static String getBuildTimestamp() {
		return prop.getProperty(PROP_BUILD_TIMESTAMP);
	}
	
}
