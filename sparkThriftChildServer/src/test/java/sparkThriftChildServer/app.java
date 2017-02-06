package sparkThriftChildServer;

import java.text.SimpleDateFormat;
import java.util.Date;

import sun.misc.Signal;
import sun.misc.SignalHandler;

@SuppressWarnings("restriction")
public class app implements SignalHandler {

	private void signalCallback(Signal sn) {
		System.out.println(sn.getName() + "is recevied.");
	}

	public void handle(Signal signalName) {
		signalCallback(signalName);
	}

	public static void main(String[] args) throws InterruptedException {
		Date dNow = new Date();
	    SimpleDateFormat ft = new SimpleDateFormat ("yyyy-MM-dd hh:mm:ss,S");
	    System.out.println("Current Date: " + ft.format(dNow));
		
	}

}
