package tbrugz.sqldump.test;

import org.junit.runner.RunWith;
import org.junit.runners.Suite;
import org.junit.runners.Suite.SuiteClasses;

import tbrugz.sqldiff.test.SQLDiffTest;
import tbrugz.sqldump.datadump.DataDumpTest;

@RunWith(Suite.class)
@SuiteClasses({
	SQLTokenizerTest.class,
	DataDumpTest.class,
	SQLDiffTest.class,
})
public class SQLDumpTestSuite {

}
