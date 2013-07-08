package asyncnode;

public class ExceptionHandler {
	public void handleException(Exception ex) throws Exception
	{
		throw ex;
	}
	
	public void safeHandleException(Exception ex)
	{
		ex.printStackTrace();
	}
}
