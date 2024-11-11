package tbrugz.sqldump.util;

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class ShutdownManager {
	
	static final Log log = LogFactory.getLog(ShutdownManager.class);
	
	static final ShutdownManager shutdownManager = new ShutdownManager();

	final List<Thread> shutdownThreads = new ArrayList<Thread>();
	
	public static ShutdownManager instance() {
		return shutdownManager;
	}

	public boolean addShutdownHook(Thread shutdownThread) {
		try {
			Runtime.getRuntime().addShutdownHook(shutdownThread);
			shutdownThreads.add(shutdownThread);
			return true;
		}
		catch(RuntimeException e) {
			log.warn("Error adding shutdown hook: "+e);
			return false;
		}
	}

	public boolean removeShutdownHook(Thread shutdownThread) {
		try {
			Runtime.getRuntime().removeShutdownHook(shutdownThread);
			shutdownThreads.remove(shutdownThread);
			return true;
		}
		catch(RuntimeException e) {
			log.warn("Error removing shutdown hook: "+e);
			return false;
		}
	}
	
	public void removeAllHooks() {
		for(Thread t: shutdownThreads) {
			Runtime.getRuntime().removeShutdownHook(t);
		}
	}

}
