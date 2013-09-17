package tbrugz.sqldump.ant;

import java.io.File;

import org.apache.tools.ant.BuildException;
import org.junit.Assert;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runners.MethodSorters;

import tbrugz.sqldump.def.ProcessingException;

@FixMethodOrder(MethodSorters.NAME_ASCENDING)
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
	public void testDump() {
		RunAnt.runAnt(file, "dump-nofail");
	}

	@Test
	public void testDumpErrorJavaOk() {
		RunAnt.runAnt(file, "dump-error-java-ok");
	}
	
	@Test(expected=RuntimeException.class)
	public void testDumpFail2() {
		RunAnt.runAnt(file, "dump-fail-2");
	}

	@Test
	public void testDumpOkLib() {
		RunAnt.runAnt(file, "dump-ok-lib");
	}

	@Test(expected=BuildException.class)
	public void testDump2Fail() {
		RunAnt.runAnt(file, "dump-2-fail");
	}

	@Test(expected=BuildException.class)
	public void testRunFail() {
		RunAnt.runAnt(file, "run-fail");
	}

	@Test(expected=BuildException.class)
	public void testRunLibFail() {
		RunAnt.runAnt(file, "run-lib-fail");
	}

	@Test(expected=BuildException.class)
	public void testDiffFail() {
		RunAnt.runAnt(file, "diff-fail");
	}

	@Test(expected=BuildException.class)
	public void testDiffLibFail() {
		RunAnt.runAnt(file, "diff-lib-fail");
	}

	@Test
	public void testDiff2QLib() {
		RunAnt.runAnt(file, "diff2q-lib");
	}

	@Test(expected=BuildException.class)
	public void testDiff2QLibFail() {
		RunAnt.runAnt(file, "diff2q-lib-fail");
	}
	
	@Test
	public void testDiff2Q() {
		RunAnt.runAnt(file, "diff2q");
	}

	@Test(expected=BuildException.class)
	public void testDiff2QFail() {
		RunAnt.runAnt(file, "diff2q-fail");
	}

	@Test(expected=BuildException.class)
	public void testDiff2Q2TargetsFail() {
		RunAnt.runAnt(file, "diff2q-2targets-fail");
	}
	
}