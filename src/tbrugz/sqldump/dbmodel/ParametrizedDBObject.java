package tbrugz.sqldump.dbmodel;

import java.util.List;

public interface ParametrizedDBObject {

	public Integer getParameterCount();
	
	public List<String> getParameterTypes();
	
}
