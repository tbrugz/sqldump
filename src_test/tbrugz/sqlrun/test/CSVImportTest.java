package tbrugz.sqlrun.test;

import java.net.URL;
import java.net.URLClassLoader;

import org.junit.Ignore;
import org.junit.Test;

import tbrugz.sqldump.sqlrun.SQLRun;

public class CSVImportTest {
	
	public static final String H2_JAR_PATH = "lib/h2-1.3.168.jar";
	
	@Test @Ignore
	public void doImportWithNewClassLoader() throws Exception {
		String userdir = System.getProperty("user.dir");
		//ClassLoader loader = loadJar(new URL("file://D:/proj/sqldump/"+H2_JAR_PATH));
		String url = "file://"+userdir+"/"+H2_JAR_PATH;
		System.out.println("user.dir: "+userdir+" ; url: "+url);
		ClassLoader loader = loadJar(new URL(url));
		Class<?> clazz = Class.forName("org.h2.Driver", true, loader);
		
		String[] params = {"-propfile=test/sqlrun-h2-csv.properties"};
		SQLRun.main(params);
	}

	@Test
	public void doImport() throws Exception {
		String[] params = {"-propfile=test/sqlrun-h2-csv.properties"};
		SQLRun.main(params);
	}
	
	static ClassLoader loadJar(URL url) {
		ClassLoader loader = URLClassLoader.newInstance(
			new URL[] { url },
			CSVImportTest.class.getClassLoader()
		);
		return loader;
	}
}
