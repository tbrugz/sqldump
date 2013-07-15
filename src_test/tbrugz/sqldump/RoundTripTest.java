package tbrugz.sqldump;

import java.io.IOException;
import java.sql.SQLException;

import javax.naming.NamingException;
import javax.xml.bind.JAXBException;
import javax.xml.stream.XMLStreamException;

import org.codehaus.jettison.json.JSONException;
import org.junit.Test;

import tbrugz.sqldiff.SQLDiff;
import tbrugz.sqldump.sqlrun.SQLRun;

public class RoundTripTest {

	/*
	full test: 
		1- dump jaxb model
		2- load into different databases (sqlrun)
		3- dump from all databases
		4- compare with original (sqldiff)
	*/
	@Test
	public void testRoundtrip() throws ClassNotFoundException, SQLException, NamingException, IOException, JAXBException, JSONException, XMLStreamException {
		//<echo message="roundtrip1: phase 1: dump [jaxb]" />
		String[] args1 = { "-propfile=test/r1-sqldump-jaxb-test.properties" };
		SQLDump.main(args1);
		
		//<!-- 2 -->
		//<echo message="roundtrip1: phase 2: run [derby]" />
		String[] args2a = { "-propfile=test/r2-sqlrun-derby-test.properties" };
		SQLRun.main(args2a);
		//<echo message="roundtrip1: phase 2: run [hsqldb]" />
		String[] args2b = { "-propfile=test/r2-sqlrun-hsqldb-test.properties" };
		SQLRun.main(args2b);
		
		//<!-- 3 -->
		//<echo message="roundtrip1: phase 3: dump [derby]" />
		String[] args3a = { "-propfile=test/r3-sqldump-derby-test.properties" };
		SQLDump.main(args3a);
		//<echo message="roundtrip1: phase 3: dump [hsqldb]" />
		String[] args3b = { "-propfile=test/r3-sqldump-hsqldb-test.properties" };
		SQLDump.main(args3b);
		
		//<!-- 4 -->
		//<echo message="roundtrip1: phase 4: diff [jaxb/jaxb-derby]" />
		String[] args4a = { "-propfile=test/r4-sqldiff-test1.properties" };
		SQLDiff.main(args4a);
		//<echo message="roundtrip1: phase 4: diff [jaxb/jaxb-hsqldb]" />
		String[] args4b = { "-propfile=test/r4-sqldiff-test2.properties" };
		SQLDiff.main(args4b);
		//<echo message="roundtrip1: phase 4: diff [jaxb-derby/jaxb-hsqldb]" />
		String[] args4c = { "-propfile=test/r4-sqldiff-test3.properties" };
		SQLDiff.main(args4c);
	}
}
