package tbrugz.sqldump.sqlrun;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;

import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import tbrugz.sqldump.sqlrun.tokenzr.Tokenizer;
import tbrugz.sqldump.sqlrun.tokenzr.TokenizerStrategy;

@RunWith(Parameterized.class)
public class SQLTokenizerFileTest {
	
	/*
	TokenizerStrategy:
	STMT_TOKENIZER,
	STMT_SCANNER,
	STMT_SCANNER_NG,
	STRING_SPLITTER,

	charsets: UTF-8, ISO-8859-1
	*/

	static final String dir = "src/test/resources/tbrugz/sqldump/sqlrun/";

	@Parameters
	public static Collection<TokenizerStrategy> data() {
		Collection<TokenizerStrategy> list = new ArrayList<TokenizerStrategy>();
		//list.add(TokenizerStrategy.STRING_SPLITTER); //XXX: deprecated
		list.add(TokenizerStrategy.STMT_TOKENIZER);
		list.add(TokenizerStrategy.STMT_SCANNER);
		list.add(TokenizerStrategy.STMT_SCANNER_NG);
		return list;
	}
	
	TokenizerStrategy strategy;
	
	public SQLTokenizerFileTest(TokenizerStrategy strategy) {
		this.strategy = strategy;
	}
	
	Tokenizer createTokenizer(File f, String inputEncoding) throws IOException {
		return createTokenizer(f, inputEncoding, true);
	}

	Tokenizer createTokenizer(File f, String inputEncoding, boolean split) throws IOException {
		//System.out.println("strategy: "+strategy+"; file: "+f);
		boolean escapeBackslashedApos = true;
		return TokenizerStrategy.getTokenizer(strategy, f, inputEncoding, escapeBackslashedApos, split);
	}

	@Test
	public void testUtf8() throws IOException {
		String charset = "UTF-8";
		Tokenizer p = createTokenizer(new File(dir, "tokenize-data-utf8.sql"), charset);
		//a;á;à;c;ç;e;ê;
		Assert.assertEquals(true, p.hasNext());
		Assert.assertEquals("a", p.next());

		Assert.assertEquals(true, p.hasNext());
		String token = p.next();
		//System.out.println("token["+charset+"]: "+token);
		Assert.assertEquals("á", token);

		Assert.assertEquals(true, p.hasNext());
		Assert.assertEquals("à", p.next());

		Assert.assertEquals(true, p.hasNext());
		Assert.assertEquals("c", p.next());

		Assert.assertEquals(true, p.hasNext());
		Assert.assertEquals("ç", p.next());

		Assert.assertEquals(true, p.hasNext());
		Assert.assertEquals("e", p.next());

		Assert.assertEquals(true, p.hasNext());
		Assert.assertEquals("ê", p.next());

		Assert.assertEquals(false, p.hasNext());
	}

	@Test
	public void testIso8859_1() throws IOException {
		String charset = "ISO-8859-1";
		//String charset = "UTF-8";
		Tokenizer p = createTokenizer(new File(dir, "tokenize-data-iso88591.sql"), charset);
		//a;á;à;c;ç;e;ê;

		Assert.assertEquals(true, p.hasNext());
		Assert.assertEquals("a", p.next());

		String token = p.next();
		//System.out.println("token["+charset+"]: "+token);
		Assert.assertEquals("á", token);

		Assert.assertEquals(true, p.hasNext());
		Assert.assertEquals("à", p.next());

		Assert.assertEquals(true, p.hasNext());
		Assert.assertEquals("c", p.next());

		Assert.assertEquals(true, p.hasNext());
		Assert.assertEquals("ç", p.next());

		Assert.assertEquals(true, p.hasNext());
		Assert.assertEquals("e", p.next());

		Assert.assertEquals(true, p.hasNext());
		Assert.assertEquals("ê", p.next());

		Assert.assertEquals(false, p.hasNext());
	}

	@Test
	public void testUtf8NoSplit() throws IOException {
		String charset = "UTF-8";
		Tokenizer p = createTokenizer(new File(dir, "tokenize-data-utf8.sql"), charset, false);
		//a;á;à;c;ç;e;ê;
		Assert.assertEquals(true, p.hasNext());
		Assert.assertEquals("a;á;à;c;ç;e;ê;", p.next());

		Assert.assertEquals(false, p.hasNext());
	}

}
