package activitystreamer.server;

import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.net.Socket;
import java.util.*;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import activitystreamer.util.Settings;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;


public class Control extends Thread {
    public static int LOAD_DIFF = 2;

	private static final Logger log = LogManager.getLogger();
    private static HashMap<String, ServerDetails> allServers;  // All servers in DS (server_id, server_details)
    private static HashSet<Connection> serverConnections;  // Server connections
    private static HashSet<Connection> clientConnections;  // Client connections
    private static HashSet<Connection> connections;  // All unauthorized connections (client and server)
	private static boolean term=false;
	private static Listener listener;

	private static UUID id = Settings.getServerId();

	protected static Control control = null;
	
	public static Control getInstance() {
		if(control==null){
			control=new Control();
		} 
		return control;
	}
	
	public Control() {
		// initialize the connections array

        allServers = new HashMap<>();
        serverConnections = new HashSet<>();
        clientConnections = new HashSet<>();
        connections = new HashSet<>();

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
		if(Settings.getRemoteHostname()!=null) {
            try {
                Connection otherServer = outgoingConnection(new Socket(Settings.getRemoteHostname(), Settings.getRemotePort()));

                JSONObject msg = new JSONObject();
                msg.put("command", "AUTHENTICATE");
                msg.put("secret", Settings.getSecret());

                otherServer.writeMsg(msg.toJSONString());

            } catch (IOException e) {
                log.error("failed to connect to " + Settings.getRemoteHostname() + ":" + Settings.getRemotePort() + " :" + e);
                System.exit(-1);
            }
        }
	}
	
	/*
	 * A general message used as a reply if there is anything incorrect about the message that was received. 
	 * This can be used by both clients and servers. 
	 */
	private boolean invalid_message(Connection con, String info) {
        JSONObject response = new JSONObject();

        response.put("command", "INVALID_MESSAGE");
        response.put("info", info);

        con.writeMsg(response.toJSONString());

        return true; // ensure connection always closes
    }
	
	/*
	 * Processing incoming messages from the connection.
	 * Return true if the connection should close.
	 */
	public synchronized boolean process(Connection con,String msg){

		JSONObject jsonObject;
		JSONParser parser = new JSONParser();
        JSONObject response = new JSONObject();

        boolean valid = false;

		try {
			jsonObject = (JSONObject) parser.parse(msg);
		} catch (ParseException e) {
			log.error("Cannot parse JSON object: "+ e);
            return invalid_message(con, "JSON parse error while parsing message");
		}

		if (jsonObject != null) {
		    if (jsonObject.get("command") == null) {
                return invalid_message(con, "Message received did not contain a command");
            }

			String command = jsonObject.get("command").toString();
			
			switch (command) {
				/*
				 * AUTHENTICATE is sent from one server to another always and only as the first msg when connecting. 
				 */
				case "AUTHENTICATE":
                    if (!connections.contains(con)) { // Cannot be authenticated
                        if (serverConnections.contains(con)) {
                            return invalid_message(con, "Server connection already authenticated");
                        } else {
                            return invalid_message(con, "Client connection trying to authenticate as a server");
                        }
                    }

                    if (jsonObject.get("secret") != null &&
							jsonObject.get("secret").toString().equals(Settings.getSecret())) {

                        connections.remove(con);
						serverConnections.add(con);

						log.info("Successful server authentication: " + con.getSocket());
					} else {
						log.info("Failed server authentication: " + con.getSocket());
						if (connections.contains(con)) connections.remove(con);
						JSONObject auth_failed = new JSONObject();
						auth_failed.put("command", "AUTHENTICATION_FAIL");
						auth_failed.put("info", "the supplied secret is incorrect: " + jsonObject.get("secret"));

						con.writeMsg(auth_failed.toJSONString());

                        return true; // closes connection
                    }
					break;
				
				case "AUTHENTICATION_FAIL":
					return true; // close connection

                /** LOGIN | REGISTER MESSAGES */
                case "LOGIN":
                    if (!connections.contains(con)) { // Cannot be authenticated
                        if (serverConnections.contains(con)) {
                            return invalid_message(con, "Server connection trying to login as a client");
                        } else {
                            return invalid_message(con, "Client connection already logged in");
                        }
                    }

                    //TODO(nelson): process username and secret on login
                    if (jsonObject.get("username") != null && jsonObject.get("secret") != null){

                        try {
                        	// TODO(@nelson): maybe don't hardcode the file path
//                            Object obj  = parser.parse(new FileReader("D:/MIT/comp90015/distributed-systems-1/src/activitystreamer/data/user.json"));
//
//                            JSONObject existingJson = (JSONObject) obj;
//                            JSONArray jsonArray = (JSONArray) existingJson.get("users");
//
//                            for (Object item : jsonArray) {
//                                JSONObject pair = (JSONObject) item;
//
//                                if ((pair.get("username").equals(jsonObject.get("username"))) && (pair.get("secret").equals(jsonObject.get("secret")))) {
                                    valid = true;
//                                    break;
//                                }
//                            }

                            if (valid){
                                response.put("command", "LOGIN_SUCCESS");
                                response.put("info", "logged in as user " + jsonObject.get("username"));

                                con.writeMsg(response.toJSONString());

                                connections.remove(con);
                                clientConnections.add(con);

                                // Check if got other servers with at least 2 clients less than this server (incl new one)
                                for (String id:allServers.keySet()) {
                                    ServerDetails sd = allServers.get(id);

                                    if ((clientConnections.size() - sd.load) >= LOAD_DIFF) {
                                        log.info("Sending redirect!!");

                                        response.clear();

                                        // Send REDIRECT message
                                        response.put("command", "REDIRECT");
                                        response.put("hostname", sd.hostname);
                                        response.put("port", sd.port);

                                        con.writeMsg(response.toJSONString());

                                        clientConnections.remove(con);
                                        return true; // Close connection
                                    }
                                }
                            } else {
                                response.put("command", "LOGIN_FAILED");
                                response.put("info", "attempt to login with wrong secret");

                                con.writeMsg(response.toJSONString());

								connections.remove(con);
                                return true;
                            }
                        } catch (Exception e){
                            log.info(e);
                        }
                    } else {
                        connections.remove(con);
                        return invalid_message(con,  "invalid username or secret");
                    }
                    break;
                case "REGISTER":
                    if (!connections.contains(con)) { // Cannot be authenticated
                        if (serverConnections.contains(con)) {
                            return invalid_message(con, "Server connection trying to register as a client");
                        } else {
                            return invalid_message(con, "Client connection already logged in");
                        }
                    }

                    //TODO(nelson): check if username already exist
                    log.info("Entered register");
                    if(jsonObject.get("username") != null){
                        try {
                            // TODO(@nelson): don't hardcode the file path here either
//                            Object obj  = parser.parse(new FileReader("../data/user.json"));
//
//                            JSONObject existingJson = (JSONObject) obj;
//                            JSONArray jsonArray = (JSONArray) existingJson.get("users");
//
//                            for (Object item : jsonArray){
//                                JSONObject pair = (JSONObject) item;
//
//                                if ((pair.get("username").equals(jsonObject.get("username"))) && (pair.get("secret").equals(jsonObject.get("secret")))) {
//                                    valid = true;
//                                    break;
//                                }
//                            }
//
//                            if (valid){
//                                //TODO(nelson): send REGISTER_FAILED to the client
//
//                                log.info("Username found");
//
//                                response.put("command", "REGISTER_FAILED");
//                                response.put("info", jsonObject.get("username") + " is already registered with the system");
//
//                                con.writeMsg(response.toJSONString());
//
//                                log.info("Message sent");
//
//                                connections.remove(con);
//                                return true;
//                            } else {
//                                //TODO(nelson): store username and secret then return REGISTER_SUCCESS to the client
//
//                                JSONObject newUser = new JSONObject();
//                                newUser.put("username", jsonObject.get("username"));
//                                newUser.put("secret", jsonObject.get("secret"));
//                                jsonArray.add(newUser);
//
//                                existingJson.put("users", jsonArray);
//                                log.info(existingJson);
//
//                                //TODO(NELSON): pls don't hardcode this file path
//                                try(FileWriter file = new FileWriter("../data/user.json")){
//
//                                    file.write(existingJson.toJSONString());
                                    log.info("Register success");

                                    response.put("command", "REGISTER_SUCCESS");
                                    response.put("info", "register success for " + jsonObject.get("username"));

                                    con.writeMsg(response.toJSONString());

                                    log.info("Message sent");
//                                }
//                            }

                        } catch (Exception e){
                            log.info(e);

                            connections.remove(con);
                            return true;
                        }
                    } else {
                        connections.remove(con);
                        return invalid_message(con, "no username specified");
                    }
                    break;
                case "LOGOUT":
                    if (clientConnections.contains(con)) clientConnections.remove(con); // TODO(Nelson) if doesn't contain, should send back INVALID MSG?
                    if (connections.contains(con)) connections.remove(con);
                    return true;

                /** SERVER ANNOUNCEMENT MESSAGES */
                case "SERVER_ANNOUNCE":
                    if (!serverConnections.contains(con)) return invalid_message(con, "Message sent from unauthenticated server");

                    if (jsonObject.get("hostname") == null) return invalid_message(con, "Server Announcement message missing hostname field");
                    if (jsonObject.get("port") == null) return invalid_message(con, "Server Announcement message missing port field");
                    if (jsonObject.get("id") == null) return invalid_message(con, "Server Announcement message missing ID field");
                    if (jsonObject.get("load") == null) return invalid_message(con, "Server Announcement message missing load field");

                    String conHostname = jsonObject.get("hostname").toString();
                    String conPort = jsonObject.get("port").toString();
                    String conId = jsonObject.get("id").toString();
                    String conLoad = jsonObject.get("load").toString();

                    ServerDetails sd = new ServerDetails(conHostname, new Integer(conPort), conId, new Integer(conLoad));

                    // Updates client load in ServerConnections
                    allServers.put(conId, sd);

                    // Forward this message to everyone else except this connection
                    for (Connection server:serverConnections) {
                        if (server == con) continue;
                        server.writeMsg(msg);
                    }
                    break;
                /**
                 * Broadcast from a server to all other servers (only between servers), to indicate that a client is trying to
                 * register a username with a given secret.
                 */
                case "LOCK_REQUEST":
                	// TODO Jason
                	// Broadcast a lock_denied to all other servers, if username is already known to the server with a different secret. 
                	
                	/* Broadcast a LOCK_ALLOWED to all other servers (between servers only) if the username is not already known 
                	 * to the server. The server will record this username an d secret pair in its local storage. 
                	 */
                	
                	/*Send an INVALID_MESSAGE if anything is incorrect about the mssage or if it receives a LOCK_REQUEST from an 
                	 * unauthenticated server (the sender has not authenticated with the server secret). The connection is close
                	 */
                	break;

				default:
                    invalid_message(con, "Invalid command received."); 
                    return true;
			}
		}

		return false;
	}
	
	/*
	 * The connection has been closed by the other party.
	 */
	public synchronized void connectionClosed(Connection con){
		if(!term) {
            if (connections.contains(con)) connections.remove(con);
            if (clientConnections.contains(con)) clientConnections.remove(con);
            if (serverConnections.contains(con)) serverConnections.remove(con);
        }
	}
	
	/*
	 * A new incoming connection has been established, and a reference is returned to it
	 */
	public synchronized Connection incomingConnection(Socket s) throws IOException{
		log.debug("incoming connection: "+Settings.socketAddress(s));
		Connection c = new Connection(s);

		connections.add(c);
		return c;
	}
	
	/*
	 * A new outgoing connection has been established, and a reference is returned to it
	 */
	public synchronized Connection outgoingConnection(Socket s) throws IOException{
		log.debug("outgoing connection: "+Settings.socketAddress(s));
		Connection c = new Connection(s);

        serverConnections.add(c); // Outgoing connection is always a server
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
				log.debug("doing activity from: "+Settings.getLocalPort());
				term=doActivity();
			}
			
		}
		log.info("closing "+connections.size()+" connection(s)");
		// clean up
		for(Connection connection:connections){
			connection.closeCon();
		}
        for(Connection client:clientConnections){
            client.closeCon();
        }
		for(Connection server:serverConnections){
            server.closeCon();
        }

		listener.setTerm(true);
	}
	
	public boolean doActivity(){
//	    allServers.clear(); // reset it every 5 seconds (handles disconnected servers)

        JSONObject msgObj = new JSONObject();

        msgObj.put("command", "SERVER_ANNOUNCE");
        msgObj.put("id", id.toString());
        msgObj.put("load", clientConnections.size());
        msgObj.put("hostname", Settings.getLocalHostname());
        msgObj.put("port", Settings.getLocalPort());

        for (Connection server:serverConnections) {
            server.writeMsg(msgObj.toString());
        }
		return false;
	}
	
	public final void setTerm(boolean t){
		term=t;
	}
	
	public final HashSet<Connection> getConnections() {
		return connections;
	}
}
