package activitystreamer.client;

import java.io.BufferedReader;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.Socket;
import java.nio.charset.Charset;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import activitystreamer.util.Settings;
import activitystreamer.server.Connection;

public class ClientSkeleton extends Thread {
	private static final Logger log = LogManager.getLogger();
	private static ClientSkeleton clientSolution;
	private TextFrame textFrame;

    private Connection clientConnection = null;
	private Socket clientSocket = null;
//	private DataOutputStream dataOutputStream = null;

	private PrintWriter out = null;
	private BufferedReader in = null;

	public static ClientSkeleton getInstance(){
		if(clientSolution==null){
			clientSolution = new ClientSkeleton();
		}
		return clientSolution;
	}
	
	public ClientSkeleton() {
        textFrame = new TextFrame();
        start();

        try {

        	clientConnection = new Connection(new Socket(Settings.getRemoteHostname(), Settings.getRemotePort()));

			clientSocket = new Socket(Settings.getRemoteHostname(), Settings.getRemotePort());
//			clientSocket.connect(Settings.get);

			out = new PrintWriter(clientSocket.getOutputStream(), true);
			in = new BufferedReader(new InputStreamReader(clientSocket.getInputStream()));

        } catch (IOException e) {
            log.error("Cannot create new client thread: " + e);
            System.exit(-1);
        }
    }
	
	@SuppressWarnings("unchecked")
	public void sendActivityObject(JSONObject activityObj){
//		if (dataOutputStream == null) {
//			try {
//				dataOutputStream = new DataOutputStream(clientSocket.getOutputStream());
//			} catch (IOException e) {
//				log.error("Unable to create data output stream: " + e);
//				return;
//			}
//		}

//		try {
//			log.info("Sending message: "+ activityObj.toJSONString());
////			dataOutputStream.writeChars();
//			dataOutputStream.write(activityObj.toJSONString().getBytes(Charset.forName("UTF-8")));
//			dataOutputStream.flush();
//		} catch (IOException e) {
//			log.error("Cannot send activity object: " + e);
//			System.exit(-1); // TODO() not sure if need to system exit this.
//		}

		out.println(activityObj.toJSONString());

//		String fromServer;
//		while ((fromServer = in.readLine()) != null) {
//			System.out.println("Server: " + fromServer);
//			if (fromServer.equals("Bye."))
//				break;
//
//			fromUser = stdIn.readLine();
//			if (fromUser != null) {
//				System.out.println("Client: " + fromUser);
//				out.println(fromUser);
//			}
//		}

//        if (clientConnection.writeMsg(activityObj.toJSONString())) {
//            log.info("Send activity object "+activityObj);
//        } else {
//            log.error("Cannot send activity object");
//            System.exit(-1); // TODO() not sure if need to system exit this.
//        }

	}
	
	
	public void disconnect(){
//		try {
//			dataOutputStream.close();
//		} catch (IOException e) {
//			log.error("Unable to close data output stream: " + e);
//			System.exit(-1);
//		}
	}
	

	public void run(){
	}
}
