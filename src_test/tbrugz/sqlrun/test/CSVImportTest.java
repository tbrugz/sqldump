package tbrugz.sqlrun.test;

import org.junit.Test;

import tbrugz.sqldump.sqlrun.SQLRun;

public class CSVImportTest {
	@Test
	public void doimport() throws Exception {
		//System.out.println("user.dir: "+System.getProperty("user.dir"));
		//ClassLoader loader = loadJar(new URL("file://D:/proj/sqldump/lib/h2-1.3.168.jar"));
		//Class<?> clazz = Class.forName("org.h2.Driver", true, loader);
		
		String[] params = {"-propfile=test/sqlrun-h2-csv.properties"};
		SQLRun.main(params);
	}
	
	/*static ClassLoader loadJar(URL url) {
		ClassLoader loader = URLClassLoader.newInstance(
			new URL[] { url },
			SQLRun.class.getClassLoader()
		);
		return loader;
	}*/
}
