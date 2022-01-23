package tbrugz.sqldump.util;

import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.Map;

public class ReflectionUtil {

	public static String getPropertyNameFromReadMethod(Method m) {
		if(m.getParameterTypes().length!=0 || m.getReturnType()==null) {
			return null;
		}
		
		String name = m.getName();
		if(name.startsWith("get")) {
			name = name.substring(3);
		}
		else if(name.startsWith("is")) {
			name = name.substring(2);
		}
		else {
			return null;
		}
		name = Character.toLowerCase(name.charAt(0)) + name.substring(1);
		return name;
	}
	
	public static Map<String, Method> getReadPropertyMethodMap(Class<?> clazz) {
		Method[] ms = clazz.getMethods();
		Map<String, Method> ret = new HashMap<>();
		for(Method m: ms) {
			String name = getPropertyNameFromReadMethod(m);
			if(name!=null) {
				ret.put(name, m);
			}
		}
		return ret;
	}
}
