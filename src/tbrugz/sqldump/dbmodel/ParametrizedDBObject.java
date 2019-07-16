package tbrugz.sqldump.dbmodel;

import java.util.List;

public interface ParametrizedDBObject extends NamedDBObject {

	public Integer getParameterCount();
	
	public List<String> getParameterTypes();
	
}
