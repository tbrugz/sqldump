package tbrugz.sqldiff;

import java.util.Properties;

import org.junit.Test;

public class DiffTwoQueriesTest {

	@Test
	public void testNoDiff() throws Exception {
		Properties p = new Properties();
		p.load(DiffTwoQueriesTest.class.getResourceAsStream("diff2qtest.properties"));
		DiffTwoQueries d2q = new DiffTwoQueries();
		d2q.doMain(new String[]{}, p);
	}

	@Test(expected=RuntimeException.class)
	public void testError() throws Exception {
		Properties p = new Properties();
		p.load(DiffTwoQueriesTest.class.getResourceAsStream("diff2qtest.properties"));
		p.setProperty("diff2q.keycols", "idx");
		DiffTwoQueries d2q = new DiffTwoQueries();
		d2q.doMain(new String[]{}, p);
	}
}
