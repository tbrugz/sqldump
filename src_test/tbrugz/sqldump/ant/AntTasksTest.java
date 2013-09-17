package tbrugz.sqldump.ant;

import java.io.File;

import org.apache.tools.ant.BuildException;
import org.junit.Assert;
import org.junit.Test;

import tbrugz.sqldump.def.ProcessingException;

public class AntTasksTest {
	
	File file = new File("src_test/build-test.xml");

	@Test(expected=BuildException.class) //inner exception should be ProcessingException...
	public void testDumpFail() {
		try {
			RunAnt.runAnt(file, "dump-fail");
		}
		catch(BuildException e) {
			Assert.assertTrue("should be instance of ProcessingException", e.getCause() instanceof ProcessingException);
			throw e;
		}
	}

	@Test
	public void testDumpNofail() {
		RunAnt.runAnt(file, "dump-nofail");
	}

	@Test
	public void testDumpFailJavaOk() {
		RunAnt.runAnt(file, "dump-fail-java-ok");
	}
	
	@Test(expected=RuntimeException.class)
	public void testDumpFail2() {
		RunAnt.runAnt(file, "dump-fail-2");
	}

	@Test
	public void testDumpOkLib() {
		RunAnt.runAnt(file, "dump-ok-lib");
	}

	@Test
	public void testDump2() {
		RunAnt.runAnt(file, "dump-2");
	}

	@Test(expected=BuildException.class)
	public void testRun() {
		RunAnt.runAnt(file, "run-fail");
	}

	@Test(expected=BuildException.class)
	public void testRunLib() {
		RunAnt.runAnt(file, "run-lib-fail");
	}

	@Test(expected=BuildException.class)
	public void testDiff() {
		RunAnt.runAnt(file, "diff-fail");
	}

	@Test(expected=BuildException.class)
	public void testDiffLib() {
		RunAnt.runAnt(file, "diff-lib-fail");
	}

	@Test
	public void testDiff2QLib() {
		RunAnt.runAnt(file, "diff2q-lib");
	}

	@Test(expected=BuildException.class)
	public void testDiff2QLibFail() {
		RunAnt.runAnt(file, "diff2q-lib");
	}
	
}