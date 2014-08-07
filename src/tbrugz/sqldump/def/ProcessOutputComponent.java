package tbrugz.sqldump.def;

import java.io.OutputStream;
import java.io.Writer;

public interface ProcessOutputComponent extends ProcessComponent {
	
	public static final String SQL_MIME_TYPE = "text/plain"; 

	public boolean acceptsOutputWriter();
	
	public void setOutputWriter(Writer writer);

	public boolean acceptsOutputStream();
	
	public void setOutputStream(OutputStream out);
	
	public String getMimeType();

}
