package tbrugz.sqldiff;

public class ConflictingChangesException extends RuntimeException {
	private static final long serialVersionUID = 1L;

	public ConflictingChangesException(String message) {
		super(message);
	}
}
