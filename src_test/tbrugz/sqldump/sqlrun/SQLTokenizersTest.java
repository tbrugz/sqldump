package tbrugz.sqldump.sqlrun;

import java.util.ArrayList;
import java.util.Collection;

import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import tbrugz.sqldump.sqlrun.tokenzr.Tokenizer;
import tbrugz.sqldump.sqlrun.tokenzr.SQLStmtScanner;
import tbrugz.sqldump.sqlrun.tokenzr.SQLStmtTokenizer;

@RunWith(Parameterized.class)
public class SQLTokenizersTest {

	/*@Parameters
	public static Collection<Class<? extends AbstractTokenizer>> data() {
		Collection<Class<? extends AbstractTokenizer>> list = new ArrayList<Class<? extends AbstractTokenizer>>();
		list.add(SQLStmtScanner.class); list.add(SQLStmtTokenizer.class);
		return list;
	}*/

	@Parameters
	public static Collection<Object[]> data() {
		Collection<Object[]> list = new ArrayList<Object[]>();
		//list.add(new Object[]{SQLStmtScanner.class, null});
		list.add(new Object[]{SQLStmtTokenizer.class, null});
		return list;
	}
	
	Class<Tokenizer> clazz;
	
	public SQLTokenizersTest(Class<Tokenizer> clazz, Object z) {
		this.clazz = clazz;
	}
	
	static Tokenizer createTokenizer(Class<Tokenizer> clazz, String str) {
		if(clazz.equals(SQLStmtTokenizer.class)) {
			return new SQLStmtTokenizer(str);
		}
		if(clazz.equals(SQLStmtScanner.class)) {
			return new SQLStmtScanner(str);
		}
		throw new RuntimeException("unknown tokenizer: "+clazz);
	}
	
	@Test
	public void testSQLStmtTokenizer() {
		Tokenizer p = createTokenizer(clazz, "abc;cde'';eee';'");
		
		Assert.assertEquals(true, p.hasNext());
		Assert.assertEquals("abc", p.next());
		Assert.assertEquals(true, p.hasNext());
		Assert.assertEquals("cde''", p.next());
		Assert.assertEquals(true, p.hasNext());
		Assert.assertEquals("eee';'", p.next());
		Assert.assertEquals(false, p.hasNext());
	}

	@Test
	public void testTokenComment() {
		Tokenizer p = createTokenizer(clazz, "abc--;\ncde;eee';'");
		
		Assert.assertEquals("abc--;\ncde", p.next());
		Assert.assertEquals("eee';'", p.next());
		Assert.assertEquals(false, p.hasNext());
	}

	@Test
	public void testTokenCommentExtraNl() {
		Tokenizer p = createTokenizer(clazz, "abc--;\n;cde;eee");
		
		Assert.assertEquals("abc--;\n", p.next());
		Assert.assertEquals("cde", p.next());
		Assert.assertEquals("eee", p.next());
		Assert.assertEquals(false, p.hasNext());
	}
	
	@Test
	public void testTokenBlockComment() {
		Tokenizer p = createTokenizer(clazz, "abc/*;*/cde'';eee';'");
		
		Assert.assertEquals("abc/*;*/cde''", p.next());
		Assert.assertEquals("eee';'", p.next());
		Assert.assertEquals(false, p.hasNext());
	}

	@Test
	public void testTokenBlockCommentNoEnd1() {
		Tokenizer p = createTokenizer(clazz, "abc/*;*/cde'';eee';");
		
		Assert.assertEquals("abc/*;*/cde''", p.next());
		Assert.assertEquals("eee';", p.next());
		Assert.assertEquals(false, p.hasNext());
	}

	@Test
	public void testTokenBlockCommentNoEnd2() {
		Tokenizer p = createTokenizer(clazz, "eee';");
		
		Assert.assertEquals("eee';", p.next());
		Assert.assertEquals(false, p.hasNext());
	}
	
	@Test
	public void testTokenBlockComment2() {
		Tokenizer p = createTokenizer(clazz, "abc/*aa*/cde;eee");
		
		Assert.assertEquals("abc/*aa*/cde", p.next());
		Assert.assertEquals("eee", p.next());
		Assert.assertEquals(false, p.hasNext());
	}

	@Test
	public void testTokenBlockCommentErr() {
		Tokenizer p = createTokenizer(clazz, "abc;a/*a;s");
		
		Assert.assertEquals("abc", p.next());
		Assert.assertEquals("a/*a;s", p.next());
		Assert.assertEquals(false, p.hasNext());
	}
	
	@Test
	public void testTokenCommentLine1() {
		Tokenizer p = createTokenizer(clazz, "abc;eee--zz\nx;bbb");
		
		Assert.assertEquals("abc", p.next());
		Assert.assertEquals("eee--zz\nx", p.next());
		Assert.assertEquals("bbb", p.next());
		Assert.assertEquals(false, p.hasNext());
	}

	@Test
	public void testTokenCommentLine2() {
		Tokenizer p = createTokenizer(clazz, "abc;eee--zzx;ab");
		
		Assert.assertEquals("abc", p.next());
		Assert.assertEquals("eee--zzx;ab", p.next());
		Assert.assertEquals(false, p.hasNext());
	}
}
