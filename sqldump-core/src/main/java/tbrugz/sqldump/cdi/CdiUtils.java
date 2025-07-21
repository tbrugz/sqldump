package tbrugz.sqldump.cdi;

import jakarta.enterprise.inject.spi.CDI;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class CdiUtils {

	static final Log log = LogFactory.getLog(CdiUtils.class);

	static boolean cdiMayBeAvailable = true;

	public static <T> Object getClassInstance(Class<T> clazz) {
		if(clazz==null) {
			log.warn("null class");
			return null;
		}
		if(!cdiMayBeAvailable) {
			return null;
		}
		CDI<Object> cdi = null;
		try {
			// https://stackoverflow.com/questions/61333554/java-how-to-check-if-cdi-is-enabled
			cdi = CDI.current();
		} catch (IllegalStateException e) {
			cdiMayBeAvailable = false;
			log.warn("CDI not available:: IllegalStateException: "+ e);
			log.debug("CDI not available:: IllegalStateException: "+ e, e);
			return null;
		}
		try {
			//log.info("getClassInstance: class: " + clazz);
			T obj = cdi.select(clazz).get();
			//log.info("getClassInstance: obj: " + obj);
			return obj;
		} catch (RuntimeException e) {
			log.warn("Exception instantiating class: " + clazz.getName() + " ; Exception: "+ e);
			log.debug("Exception instantiating class: " + clazz.getName(), e);
		}
		return null;
	}

}
