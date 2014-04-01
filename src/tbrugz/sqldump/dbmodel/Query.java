package tbrugz.sqldump.dbmodel;

import java.util.List;
import java.util.Map;

public class Query extends View {
	private static final long serialVersionUID = 1L;

	String id;
	List<String> parameterValues;
	
	public String rsDecoratorFactoryClass;
	public Map<String,String> rsDecoratorArguments;
	
	public String getId() {
		return id;
	}

	public void setId(String id) {
		this.id = id;
	}

	public List<String> getParameterValues() {
		return parameterValues;
	}

	public void setParameterValues(List<String> parameterValues) {
		this.parameterValues = parameterValues;
	}
	
}
