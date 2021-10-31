package tbrugz.sqldump.util;

import java.io.File;
import java.io.IOException;
import java.util.List;

import org.junit.Assert;
import org.junit.Test;

public class FileUtilsTest {
	
	final File srcTestDir = new File("src_test/");
	final File srcTestSqlrunDir = new File("src_test/tbrugz/sqldump/sqlrun");

	@Test
	public void testListFilesRegex() {
		List<String> ll = FileUtils.getFilesRegex(srcTestSqlrunDir, ".*\\.csv");
		//System.out.println("testListFilesRegex:" + ll);
		Assert.assertEquals(3, ll.size()); //dept.csv, etc.csv, emp.csv
	}

	@Test
	public void testListFilesGlob() throws IOException {
		List<String> ll = FileUtils.getFilesGlobAsString(srcTestSqlrunDir, "*.csv");
		//System.out.println("testListFilesGlob:" + ll);
		Assert.assertEquals(3, ll.size()); //dept.csv, etc.csv, emp.csv
	}

	@Test
	public void testListFilesGlobWithDir() throws IOException {
		List<String> ll = FileUtils.getFilesGlobAsString(srcTestDir, "tbrugz/sqldump/sqlrun/*.csv");
		//System.out.println("testListFilesGlobWithDir:" + ll);
		Assert.assertEquals(3, ll.size()); //dept.csv, etc.csv, emp.csv
	}

	@Test
	public void testListFilesGlobWithDir2() throws IOException {
		List<String> ll = FileUtils.getFilesGlobAsString(srcTestDir, "tbrugz/sqldump/**/*.csv");
		//System.out.println("testListFilesGlobWithDir2:" + ll);
		Assert.assertEquals(4, ll.size()); //dept.csv, etc.csv, emp.csv, processors/proj.csv
	}

	@Test
	public void testListFilesGlobWithDir3() throws IOException {
		List<String> ll = FileUtils.getFilesGlobAsString(srcTestDir, "**/sqlrun/*.csv");
		//System.out.println("testListFilesGlobWithDir3:" + ll);
		Assert.assertEquals(3, ll.size()); //dept.csv, etc.csv, emp.csv
	}

	@Test
	public void testListFilesGlobWithDirFull() throws IOException {
		List<String> ll = FileUtils.getFilesGlobAsString(srcTestDir, "**/sqldump/**/*.csv");
		//System.out.println("testListFilesGlobWithDirFull:" + ll);
		Assert.assertEquals(4, ll.size()); //dept.csv, etc.csv, emp.csv, processors/proj.csv
	}
	
	/*
	public static boolean isAbsolute(String path) {
		File f = new File(path);
		return f.isAbsolute();
	}
	
	//@Test
	public void debug() {
		String s = "/home/tbrugz/proj/sqldump";
		Path p = Paths.get(s);
		System.out.println(p);
		System.out.println(p.subpath(0, 2));
	}
	*/
	

}
