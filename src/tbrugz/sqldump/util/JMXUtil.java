package tbrugz.sqldump.util;

import java.lang.management.ManagementFactory;

import javax.management.InstanceAlreadyExistsException;
import javax.management.MBeanRegistrationException;
import javax.management.MBeanServer;
import javax.management.MalformedObjectNameException;
import javax.management.NotCompliantMBeanException;
import javax.management.ObjectName;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import tbrugz.sqldump.sqlrun.jmx.SQLRMBean;

public class JMXUtil {

	static final Log log = LogFactory.getLog(SQLRMBean.class);
	
	public static void registerMBean(String mBeanName, SQLRMBean mbean) throws MalformedObjectNameException, InstanceAlreadyExistsException, MBeanRegistrationException, NotCompliantMBeanException {
		MBeanServer mbs = ManagementFactory.getPlatformMBeanServer();
		ObjectName name = new ObjectName(mBeanName);
		mbs.registerMBean(mbean, name);
	}

	public static void registerMBeanSimple(String mBeanName, SQLRMBean mbean) {
		try {
			registerMBean(mBeanName, mbean);
		} catch (Exception e) {
			log.warn("Error registering MBean '"+mBeanName+"': "+e);
			log.debug("Error registering MBean '"+mBeanName+"'", e);
		}
	}

}
