package tbrugz.sqldump;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Properties;

public class ParametrizedProperties extends Properties {

	private static final long serialVersionUID = 1L;
	
	@Override
	public String getProperty(String key, String defaultValue) {
		String val = getProperty(key);
		return (val == null) ? defaultValue : val;
	}
	
	@Override
	public String getProperty(String key) {
		String s = super.getProperty(key);
		if(s==null) {
			return null;
		}
		if(s.indexOf("${")<0) {
			return s;
		}
		
		StringBuffer sb = new StringBuffer(s);
		//s.replaceAll("\\$\\{(.*?)\\}", );
		int count = 0;
		int pos1;
		while((pos1 = sb.indexOf("${", count)) >= 0) {
			int pos2 = sb.indexOf("}", pos1);
			if(pos2<0) { break; }
			count = pos1+1;
			String prop = sb.substring(pos1+2, pos2);
			String propSuperValue = super.getProperty(prop);
			if(propSuperValue==null) {
				propSuperValue = System.getProperty(prop);
			}
			if(propSuperValue==null) { continue; }
			
			sb.replace(pos1, pos2+1, propSuperValue);
		}
		return sb.toString();
	}
	
	@Deprecated
	public static void main(String[] args) throws FileNotFoundException, IOException {
		Properties prop = new ParametrizedProperties();
		prop.load(new FileInputStream("sqldump.properties"));
		System.out.println("p: "+prop.getProperty("sqldump.zzz"));
	}
	
}
