package tbrugz.sqldump;

import org.junit.Ignore;
import org.junit.Test;

import tbrugz.sqldiff.SQLDiff;
import tbrugz.sqldump.sqlrun.SQLRun;

//@Ignore
public class RoundTripTest {

	static String DIR = "src/test/resources/roundtrip/";
	
	/*
	full test: 
		1- dump jaxb model
		2- load into different databases (sqlrun)
		3- dump from all databases
		4- compare with original (sqldiff)
	*/
	@Test
	public void testRoundtrip() throws Exception {
		//<echo message="roundtrip1: phase 1: dump [jaxb]" />
		String[] args1 = { "-propfile="+DIR+"/r1-sqldump-jaxb-test.properties" };
		SQLDump.main(args1);
		System.out.println("r1...");
		
		//<!-- 2 -->
		//<echo message="roundtrip1: phase 2: run [derby]" />
		String[] args2a = { "-propfile="+DIR+"/r2-sqlrun-derby-test.properties" };
		SQLRun.main(args2a);
		//<echo message="roundtrip1: phase 2: run [hsqldb]" />
		String[] args2b = { "-propfile="+DIR+"/r2-sqlrun-hsqldb-test.properties" };
		SQLRun.main(args2b);
		
		//<!-- 3 -->
		//<echo message="roundtrip1: phase 3: dump [derby]" />
		String[] args3a = { "-propfile="+DIR+"/r3-sqldump-derby-test.properties" };
		SQLDump.main(args3a);
		//<echo message="roundtrip1: phase 3: dump [hsqldb]" />
		String[] args3b = { "-propfile="+DIR+"/r3-sqldump-hsqldb-test.properties" };
		SQLDump.main(args3b);
		
		//<!-- 4 -->
		//<echo message="roundtrip1: phase 4: diff [jaxb/jaxb-derby]" />
		String[] args4a = { "-propfile="+DIR+"/r4-sqldiff-test1.properties" };
		SQLDiff.main(args4a);
		//<echo message="roundtrip1: phase 4: diff [jaxb/jaxb-hsqldb]" />
		String[] args4b = { "-propfile="+DIR+"/r4-sqldiff-test2.properties" };
		SQLDiff.main(args4b);
		//<echo message="roundtrip1: phase 4: diff [jaxb-derby/jaxb-hsqldb]" />
		String[] args4c = { "-propfile="+DIR+"/r4-sqldiff-test3.properties" };
		SQLDiff.main(args4c);
	}
}
