package tbrugz.sqldump.util;

import java.io.IOException;
import java.util.Properties;

public class Version {

	final static String PROP_VERSION = "version";
	
	final static Properties prop = new Properties();
	
	static {
		try {
			prop.load(Version.class.getResourceAsStream("/version.properties"));
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	public static String getVersion() {
		return prop.getProperty(PROP_VERSION);
	}
}
