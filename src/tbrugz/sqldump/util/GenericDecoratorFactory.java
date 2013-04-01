package tbrugz.sqldump.util;

//XXX: better class name is welcome
public interface GenericDecoratorFactory<T> {

	void set(String key, String value);
	
	T getDecoratorOf(T arg);
}
