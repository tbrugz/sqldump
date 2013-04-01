package tbrugz.sqldump.dbmodel;

import java.util.List;
import java.util.Map;

public class Query extends View {
	private static final long serialVersionUID = 1L;

	public String id;
	public List<String> parameterValues;
	public Integer parameterCount;
	
	public String rsDecoratorFactoryClass;
	public Map<String,String> rsDecoratorArguments;
}
