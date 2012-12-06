package tbrugz.sqldump;

import org.junit.runner.RunWith;
import org.junit.runners.Suite;
import org.junit.runners.Suite.SuiteClasses;

import tbrugz.sqldiff.SQLDiff;
import tbrugz.sqldiff.test.SQLDiffTest;
import tbrugz.sqldump.datadump.DataDumpTest;
import tbrugz.sqldump.test.SQLDumpTestSuite;
import tbrugz.sqldump.util.ParametrizedProperties;
import tbrugz.sqldump.util.ParametrizedPropertiesTest;
import tbrugz.sqlrun.test.CSVImportTest;

@RunWith(Suite.class)
@SuiteClasses({
	CSVImportTest.class,
	DataDumpTest.class,
	ParametrizedPropertiesTest.class,
	SQLDumpTestSuite.class,
	SQLDiffTest.class,
})
public class AllTestSuite {

}
