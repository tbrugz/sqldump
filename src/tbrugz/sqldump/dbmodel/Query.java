package tbrugz.sqldump.dbmodel;

import java.util.List;
import java.util.Map;

public class Query extends View {
	private static final long serialVersionUID = 1L;

	String id;
	List<Object> parameterValues;

	String rsDecoratorFactoryClass;
	Map<String,String> rsDecoratorArguments;
	
	public String getId() {
		return id;
	}

	public void setId(String id) {
		this.id = id;
	}

	public List<Object> getParameterValues() {
		return parameterValues;
	}

	public void setParameterValues(List<Object> parameterValues) {
		this.parameterValues = parameterValues;
	}
	
	@Override
	public String getRelationType() {
		return "query";
	}

	public String getRsDecoratorFactoryClass() {
		return rsDecoratorFactoryClass;
	}

	public void setRsDecoratorFactoryClass(String rsDecoratorFactoryClass) {
		this.rsDecoratorFactoryClass = rsDecoratorFactoryClass;
	}

	public Map<String, String> getRsDecoratorArguments() {
		return rsDecoratorArguments;
	}

	public void setRsDecoratorArguments(Map<String, String> rsDecoratorArguments) {
		this.rsDecoratorArguments = rsDecoratorArguments;
	}
	
}
