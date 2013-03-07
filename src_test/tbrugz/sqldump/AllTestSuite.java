package tbrugz.sqldump;

import org.junit.runner.RunWith;
import org.junit.runners.Suite;
import org.junit.runners.Suite.SuiteClasses;

import tbrugz.sqldiff.test.DerbyDiffTest;
import tbrugz.sqldiff.test.DiffFromJAXB;
import tbrugz.sqldiff.test.HSQLDBDiffTest;
import tbrugz.sqldiff.test.SQLDiffTest;
import tbrugz.sqldump.datadump.DataDumpTest;
import tbrugz.sqldump.dbmodel.ColTypeUtilTest;
import tbrugz.sqldump.def.AbstractFailable;
import tbrugz.sqldump.sqlrun.CSVImportTest;
import tbrugz.sqldump.sqlrun.FailoverTest;
import tbrugz.sqldump.sqlrun.SQLRunAndDumpTest;
import tbrugz.sqldump.test.SQLTokenizerTest;
import tbrugz.sqldump.util.ParametrizedPropertiesTest;

@RunWith(Suite.class)
@SuiteClasses({
	//"unit" tests
	DataDumpTest.class,
	ParametrizedPropertiesTest.class,
	SQLTokenizerTest.class,
	ColTypeUtilTest.class,

	//import
	CSVImportTest.class,
	
	//run
	FailoverTest.class,

	//dump ?
	//SQLDumpTestSuite.class,
	
	//diff
	DiffFromJAXB.class,

	//diff + database
	SQLDiffTest.class,
	DerbyDiffTest.class,
	HSQLDBDiffTest.class,
	
	//roundtrip
	SQLRunAndDumpTest.class,
	//RoundTripTest.class,
})
public class AllTestSuite {
	static {
		AbstractFailable.DEFAULT_FAILONERROR = true;		
	}
}
