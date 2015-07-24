package tbrugz.sqldump.def;

import java.io.OutputStream;
import java.io.Writer;

public interface ProcessOutputComponent extends ProcessComponent {
	
	/*
	 *  see:
	 *  http://www.iana.org/assignments/media-types/media-types.xhtml
	 *  http://www.iana.org/assignments/media-types/application/sql
	 */
	public static final String SQL_MIME_TYPE = "application/sql"; 

	public boolean acceptsOutputWriter();
	
	public void setOutputWriter(Writer writer);

	public boolean acceptsOutputStream();
	
	public void setOutputStream(OutputStream out);
	
	public String getMimeType();

}
