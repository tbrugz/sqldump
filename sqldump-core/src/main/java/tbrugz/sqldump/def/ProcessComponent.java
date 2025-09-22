package tbrugz.sqldump.def;

import java.util.Properties;

public interface ProcessComponent {

	public void setProperties(Properties prop);

	public void setPropertiesPrefix(String propertiesPrefix);
	
	public void setFailOnError(boolean failonerror);

	public void setId(String processorId);

	public String getId();

}
