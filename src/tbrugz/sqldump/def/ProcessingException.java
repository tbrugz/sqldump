package tbrugz.sqldump.def;

public class ProcessingException extends RuntimeException {
	private static final long serialVersionUID = 1L;
	
	public ProcessingException(String message) {
		super(message);
	}

	public ProcessingException(String message, Throwable cause) {
		super(message, cause);
	}

	public ProcessingException(Throwable cause) {
		super(cause);
	}
	
}
