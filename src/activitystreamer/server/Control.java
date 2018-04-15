package activitystreamer.server;

import java.io.IOException;
import java.net.Socket;
import java.util.ArrayList;
import java.util.HashMap;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import activitystreamer.util.Settings;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

public class Control extends Thread {
	private static final Logger log = LogManager.getLogger();
	// Connections are just client connections, not server connections
    private static HashMap<String, Connection> serverConnections;  // Incoming server connections
    private static HashMap<String, Connection> clientConnections;  // Incoming client connections
    private static ArrayList<Connection> connections;  // All unauthorised connections
	private static boolean term=false;
	private static Listener listener;

	private static String id = "1";
	
	protected static Control control = null;
	
	public static Control getInstance() {
		if(control==null){
			control=new Control();
		} 
		return control;
	}
	
	public Control() {
		// initialize the connections array
		serverConnections = new HashMap<String, Connection>();
		connections = new ArrayList<Connection>();

		// connect to another server
		initiateConnection();

		// start a listener
		try {
			listener = new Listener();
		} catch (IOException e1) {
			log.fatal("failed to startup a listening thread: "+e1);
			System.exit(-1);
		}	
	}
	
	public void initiateConnection(){
		// make a connection to another server if remote hostname is supplied
		if(Settings.getRemoteHostname()!=null){
			try {
			    // TODO(joanna): send AUTHENTICATE message to other server after connection and check secret
				// TODO(joanna): Original server gets AUTHENTICATE message and if it's wrong, close connection. Ignore every other message.

                //Settings.getSecret();

				Connection otherServer = outgoingConnection(new Socket(Settings.getRemoteHostname(),Settings.getRemotePort()));

				JSONObject msg = new JSONObject();
				msg.put("command", "AUTHENTICATE");
				msg.put("secret", Settings.getSecret());

				otherServer.writeMsg(msg.toJSONString());

			} catch (IOException e) {
				log.error("failed to connect to "+Settings.getRemoteHostname()+":"+Settings.getRemotePort()+" :"+e);
				System.exit(-1);
			}
		}
	}
	
	/*
	 * Processing incoming messages from the connection.
	 * Return true if the connection should close.
	 */
	public synchronized boolean process(Connection con,String msg){
	    log.debug("processing: "+msg+ " from " + con.getSocket());
		// TODO() do some error handling here for the messages
	    // return true;

        //TODO() check if is a client or server

		JSONObject jsonObject;
		JSONParser parser = new JSONParser();
		try {
			jsonObject = (JSONObject) parser.parse(msg);
		} catch (ParseException e) {
			log.error("Cannot parse JSON object: "+ e);
			return false; //TODO() maybe send back an error message?
		}

		if (jsonObject != null) {
		    if (jsonObject.get("command") == null) {
                log.error("Invalid message");
                //TODO() send back Invalid message
                return true;
            }

			String command = jsonObject.get("command").toString();

			switch (command) {
				case "AUTHENTICATE":
                    if (jsonObject.get("secret") != null &&
							jsonObject.get("secret").toString().equals(Settings.getSecret())) {

                    	if (!connections.contains(con)) {
                    		// TODO() Already authenticated, send back invalid message
							//serverConnections.remove(con); // TODO() check if con is key or value?
							return true;
						}
                        serverConnections.put(id+1, con); // Server connection is authenticated
                        connections.remove(con);

						log.info("Successful server authentication: " + con.getSocket());
					} else {
						log.info("Failed server authentication: " + con.getSocket());
						if (connections.contains(con)) connections.remove(con);

						JSONObject auth_failed = new JSONObject();
						auth_failed.put("command", "AUTHENTICATION_FAIL");
						auth_failed.put("info", "the supplied secret is incorrect: " + jsonObject.get("secret"));

						con.writeMsg(auth_failed.toJSONString());

                        return true; // close connection
                    }
					break;
				case "AUTHENTICATION_FAIL":
					return true; // close connection
				default:
                    // TODO() send back invalid message
                    return true;
			}
		}

		return false;
	}
	
	/*
	 * The connection has been closed by the other party.
	 */
	public synchronized void connectionClosed(Connection con){
		if(!term) connections.remove(con);
	}
	
	/*
	 * A new incoming connection has been established, and a reference is returned to it
	 */
	public synchronized Connection incomingConnection(Socket s) throws IOException{
		log.debug("incoming connection: "+Settings.socketAddress(s));
		Connection c = new Connection(s);
		//TODO() how to check if connections is a server or a client??
        log.debug("incoming connection: "+s.toString());

		connections.add(c);
		return c;
	}
	
	/*
	 * A new outgoing connection has been established, and a reference is returned to it
	 */
	public synchronized Connection outgoingConnection(Socket s) throws IOException{
		log.debug("outgoing connection: "+Settings.socketAddress(s));
		Connection c = new Connection(s);
		connections.add(c);
		return c;
		
	}
	
	@Override
	public void run(){
		log.info("using activity interval of "+Settings.getActivityInterval()+" milliseconds");
		while(!term){
			// do something with 5 second intervals in between
			try {
				Thread.sleep(Settings.getActivityInterval());
			} catch (InterruptedException e) {
				log.info("received an interrupt, system is shutting down");
				break;
			}
			if(!term){
				log.debug("doing activity.");
				term=doActivity();
			}
			
		}
		log.info("closing "+connections.size()+" connection(s)");
		// clean up
		for(Connection connection : connections){
			connection.closeCon();
		}
		listener.setTerm(true);
	}
	
	public boolean doActivity(){
		return false;
	}
	
	public final void setTerm(boolean t){
		term=t;
	}
	
	public final ArrayList<Connection> getConnections() {
		return connections;
	}
}