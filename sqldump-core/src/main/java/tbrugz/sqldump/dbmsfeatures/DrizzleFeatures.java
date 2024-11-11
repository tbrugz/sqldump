package tbrugz.sqldump.dbmsfeatures;

import java.util.Properties;

public class DrizzleFeatures extends MySQLFeatures {

	@Override
	public void procProperties(Properties prop) {
		super.procProperties(prop);
		
		grabViews = false;
		grabTriggers = false;
		grabExecutables = false;
	}
	
}
