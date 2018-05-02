package tbrugz.sqldump.datadump;

import java.util.Properties;

/*
 * http://talk.commonmark.org/t/tables-in-pure-markdown/81
 * https://github.com/gitlabhq/gitlabhq/blob/master/doc/markdown/markdown.md#tables
 * https://help.github.com/articles/organizing-information-with-tables/
 * https://michelf.ca/projects/php-markdown/extra/#table
 * 
 * TODO: escape "|" with "\" (see: https://help.github.com/articles/organizing-information-with-tables/)
 */
public class MarkdownSyntax extends FFCDataDump {

	static final String MARKDOWN_SYNTAX_ID = "markdown";
	static final String MARKDOWN_MIMETYPE = "text/markdown";
	
	boolean useTextPlainMimeType = false;
	
	@Override
	public String getSyntaxId() { 
		return MARKDOWN_SYNTAX_ID;
	}
	
	@Override
	public String getDefaultFileExtension() {
		return "md";
	}
	
	// http://stackoverflow.com/questions/10701983/what-is-the-mime-type-for-markdown
	@Override
	public String getMimeType() {
		if(useTextPlainMimeType) {
			return PLAINTEXT_MIMETYPE;
		}
		return MARKDOWN_MIMETYPE;
	}
	
	@Override
	public void procProperties(Properties prop) {
		super.procProperties(prop);
		
		repeatHeader = false;
		showColNamesUpperLine = false;
		showTrailerLine = false;
		show1stColNamesUpperLine = showColNamesUpperLine;
		
		separator = " | ";
		colNamesLineCrossSep = " | ";

		firstColSep = "| ";
		firstPositionSeparator = "| ";

		lastPositionSeparator = " |";
		colNamesLineLastCrossSep = " |";

		headerLine1stSep = firstColSep;
		headerLineMiddleSep = separator;
		headerLineLastSep = lastPositionSeparator;
		
		footerLine1stSep = firstColSep;
		footerLineMiddleSep = separator;
		footerLineLastSep = lastPositionSeparator;
		
		//colNamesLineSep = "─";
		
		/*
		// box example:
		// ─ │ ┌ ┐ └ ┘ ├ ┤ ┬ ┴ ┼
		
		repeatHeader = true;
		showColNamesUpperLine = true;
		showTrailerLine = true;
		
		colNamesLineSep = "─";
		
		separator = "│";
		colNamesLineCrossSep = "│";

		firstColSep = "│";
		firstPositionSeparator = "│";

		lastPositionSeparator = "│";
		colNamesLineLastCrossSep = "│";
		
		headerLine1stSep = "┌";
		headerLineMiddleSep = "┬";
		headerLineLastSep = "┐";

		firstColSep = "├";
		colNamesLineCrossSep = "┼";
		colNamesLineLastCrossSep = "┤";
		
		footerLine1stSep = "└";
		footerLineMiddleSep = "┴";
		footerLineLastSep = "┘";

		//firstColSep = "┌";
		//colNamesLineCrossSep = "┬";
		//colNamesLineLastCrossSep = "┐";
		
		*/
		
		validateProperties();
	}
	
	void appendColNamesLowerLine(StringBuilder sb, boolean isBlock1stLine) {
		if(show1stColSeparator) { sb.append(firstColSep); }
		
		for(int j=0;j<numCol;j++) {
			String sep = j + 1 < numCol ? colNamesLineCrossSep : colNamesLineLastCrossSep;
			if(leftAlignField.get(j)) {
				sb.append(":");
				appendPattern(sb, colsMaxLenght.get(j)-1, colNamesLineSep, sep);
			}
			else {
				appendPattern(sb, colsMaxLenght.get(j)-1, colNamesLineSep, ":" + sep);
			}
		}
		sb.append(recordDemimiter);
	}
	
}
