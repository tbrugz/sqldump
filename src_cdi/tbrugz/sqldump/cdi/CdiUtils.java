package tbrugz.sqldump.cdi;

import javax.enterprise.inject.spi.CDI;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class CdiUtils {

	static final Log log = LogFactory.getLog(CdiUtils.class);

	public static <T> Object getClassInstance(Class<T> clazz) {
		try {
			//log.info("getClassInstance: class: " + clazz);
			T obj = CDI.current().select(clazz).get();
			//log.info("getClassInstance: obj: " + obj);
			return obj;
		} catch (RuntimeException e) {
			log.warn("Exception instantiating class: " + clazz.getName() + " ; Exception: "+ e);
			log.debug("Exception instantiating class: " + clazz.getName(), e);
		}
		return null;
	}

}
