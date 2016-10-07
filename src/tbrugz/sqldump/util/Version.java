package tbrugz.sqldump.util;

import java.io.IOException;
import java.util.Properties;

public class Version {

	static final String PROP_VERSION = "version";
	static final String PROP_BUILD_NUMBER = "build.revisionNumber";
	static final String PROP_BUILD_TIMESTAMP = "build.timestamp";
	
	static final Properties prop = new Properties();
	
	static {
		try {
			prop.load(Version.class.getResourceAsStream("/sqldump-version.properties"));
		} catch (IOException e) {
			e.printStackTrace();
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
