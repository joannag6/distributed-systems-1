package activitystreamer.client;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.Socket;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.json.simple.JSONObject;

import activitystreamer.util.Settings;

public class ClientSkeleton extends Thread {
	private static final Logger log = LogManager.getLogger();
	private static ClientSkeleton clientSolution;
	private TextFrame textFrame;

	private Socket clientSocket = null;

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
			clientSocket = new Socket(Settings.getRemoteHostname(), Settings.getRemotePort());

			out = new PrintWriter(clientSocket.getOutputStream(), true);
			in = new BufferedReader(new InputStreamReader(clientSocket.getInputStream()));

        } catch (IOException e) {
            log.error("Cannot create new client thread: " + e);
            System.exit(-1);
        }
    }
	
	@SuppressWarnings("unchecked")
	public void sendActivityObject(JSONObject activityObj){
		out.println(activityObj.toJSONString());
	}
	
	
	public void disconnect(){
	}
	

	public void run(){
	}
}
