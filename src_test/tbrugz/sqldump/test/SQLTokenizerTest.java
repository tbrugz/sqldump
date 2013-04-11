package tbrugz.sqldump.test;
import org.junit.Test;

import tbrugz.sqldump.sqlrun.SQLStmtTokenizer;

@Deprecated
public class SQLTokenizerTest {

	@Test
	public void testTokenizer() {
		SQLStmtTokenizer p = new SQLStmtTokenizer("abc;cde'';eee';'");
		int count = 0;
		for(String s: p) {
			System.out.println("s: "+s);
			count++;
			if(count==10) { break; }
		}
	}
	
}
