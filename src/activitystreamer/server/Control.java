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
    private static HashMap<Connection, ClientDetails> clientConnections;  // Client connections 
    private static HashSet<Connection> connections;  // All initial, unauthorized connections (client and server)
	private static boolean term=false;


	private static HashMap<String,String> userData; 


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
        clientConnections = new HashMap<>();
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

    /**
     * Process activity object by adding a authenticated_user field. Also checks that it's a valid JSON object.
     * Returns the processed activity object as a string. Returns null if something goes wrong and connection
     * should be closed.
     */
    private String process_activity_object(Connection con, String jsonString) {
        JSONObject jsonObject;
        JSONParser parser = new JSONParser();

        try {
            jsonObject = (JSONObject) parser.parse(jsonString);
        } catch (ParseException e) {
            log.error("Cannot parse JSON object: "+ e);
            invalid_message(con, "JSON parse error while parsing activity object");
            return null;
        }

        if (jsonObject != null) {
            jsonObject.put("authenticated_user", clientConnections.get(con).username);
        } else {
            log.error("Activity JSON object is null");
            invalid_message(con, "Activity JSON object is null");
            return null;
        }

	    return jsonObject.toJSONString(); // all g, return null if something bad happens
    }

	/*
	 * A general message used as a reply if there is anything incorrect about the message that was received. 
	 * This can be used by both clients and servers. 
	 */
	private boolean invalid_message(Connection con, String info) {
        JSONObject response = new JSONObject();

        response.put("command", "INVALID_MESSAGE");
        response.put("info", info);
        log.info("invalid message happened, " + info); //TODO, unsure if we need this to be here, but a log message would be helpful.
        con.writeMsg(response.toJSONString());

        return true; // ensure connection always closes
    }

    /*
     * Redirect connection to another server to balance load on server
     */
    private boolean redirect(Connection con, JSONObject response){
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

        return false;
    }

    /*
     * A general message used as a reply if there is a problem with authentication.
     * This can be used by both clients and servers.
     */
    private boolean auth_failed(Connection con, String info) {
        JSONObject response = new JSONObject();

        response.put("command", "AUTHENTICATION_FAIL");
        response.put("info", info);

        con.writeMsg(response.toJSONString());
        return true;
    }
	
	/*
	 * Processing incoming messages from the connection.
	 * Return true if the connection should close.
	 */
	public synchronized boolean process(Connection con,String msg){

		JSONObject jsonObject;
		JSONParser parser = new JSONParser();
        JSONObject response = new JSONObject();

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
					/* if it not in our list of current unauthorized connections, it is either a server that is already authenticated
					 * trying to reauthenticate, or a client trying to authenticate as a server. In both cases, we return invalid_message. 
					 */
					if (!connections.contains(con)) { 
                        if (serverConnections.contains(con)) {
                            return invalid_message(con, "Server connection already authenticated");
                        } else {
                            return invalid_message(con, "Client connection trying to authenticate as a server");
                        }
                    }

					/* If it is in our list of current unauthorized connections, if it has a secret and it is the right secret, we
					 * add it to our server list. Otherwise, we remove it from unauthorized connections and then return auth_failed
					 * to close the connection.
                     */
                    if (jsonObject.get("secret") != null &&
							jsonObject.get("secret").toString().equals(Settings.getSecret())) {

                        connections.remove(con);
						serverConnections.add(con);

						log.info("Successful server authentication: " + con.getSocket());
					} else {
						log.info("Failed server authentication: " + con.getSocket());
						if (connections.contains(con)) connections.remove(con);

						return auth_failed(con, "the supplied secret is incorrect: " + jsonObject.get("secret"));
                    }
					break;
				
				case "AUTHENTICATION_FAIL":
					return true; // close connection

                /** LOGIN | REGISTER MESSAGES */
                case "LOGIN":
                	/*
                	 * If connection not in unauthorized connections, it is either a server trying to login as client or 
                	 * a client that is already logged in. In both cases, we return invalid_message.
                	 */
                    if (!connections.contains(con)) { // Cannot be authenticated
                        if (serverConnections.contains(con)) {
                            return invalid_message(con, "Server connection trying to login as a client");
                        } else {
                            return invalid_message(con, "Client connection already logged in");
                        }
                    }


                    /* 
                     * If connection is in unauthorized connections, we have to see if the username and secret is not null. If both not null, 
                     * and it is a valid login attempt, we remove it from unauthorized connections and add it to clientConnections. The server
                     * follows up a LOGIN_SUCCESS message with a REDIRECT message, so we need to add the current connection to clientConnections,
                     * before we do any form of REDIRECT.
                     */
					if (jsonObject.size() > 3) return invalid_message(con, "Invalid number of arguments");

                    if (jsonObject.get("username").equals("anonymous")){
                        response.put("command", "LOGIN_SUCCESS");
                        response.put("info", "logged in as user " + jsonObject.get("username"));

                        con.writeMsg(response.toJSONString());

                        connections.remove(con);
                        clientConnections.put(con, new ClientDetails(jsonObject.get("username").toString(), ""));

                        // Check if got other servers with at least 2 clients less than this server (incl new one)
                         return redirect(con,response);
                    }


                    //TODO(nelson): process username and secret on login
                    if (jsonObject.get("username") != null && jsonObject.get("secret") != null){

                        try {
                            boolean valid = false;

                            // Read user data from local storage
                            Iterator it = userData.entrySet().iterator();

                            while (it.hasNext()){
                                HashMap.Entry pair = (HashMap.Entry) it.next();

                                // Match credentials of users
                                if ((pair.getKey().equals(jsonObject.get("username")) && pair.getValue().equals(jsonObject.get("secret")))) {
                                    valid = true;
                                    break;
                                }

                                it.remove();
                            }

                            if (valid){
                                // LOGIN SUCCESS and return message
                                response.put("command", "LOGIN_SUCCESS");
                                response.put("info", "logged in as user " + jsonObject.get("username"));

                                con.writeMsg(response.toJSONString());

                                connections.remove(con);
                                clientConnections.put(con, new ClientDetails(jsonObject.get("username").toString(), jsonObject.get("secret").toString()));

                                redirect(con,response);
                            } else {
                                // LOGIN FAILED and return message
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

                    // Handle invalid number of arguments
                    if (jsonObject.size() > 3) return invalid_message(con, "Invalid number of arguments");

                    //TODO(nelson): check if username already exist
                    log.info("Entered register");
                    if(jsonObject.get("username") != null){
                        try {
                            boolean exist = false;

                            // Read user data from local storage
                            Iterator it = userData.entrySet().iterator();

                            while (it.hasNext()){
                                HashMap.Entry pair = (HashMap.Entry) it.next();

                                if ((pair.getKey().equals(jsonObject.get("username")))) {
                                    exist = true;
                                    break;
                                }

                                it.remove();
                            }

                            if (exist){
                                //TODO(nelson): send REGISTER_FAILED to the client
                                log.info("Username found");

                                response.put("command", "REGISTER_FAILED");
                                response.put("info", jsonObject.get("username") + " is already registered with the system");

                                con.writeMsg(response.toJSONString());

                                log.info("Message sent");

                                connections.remove(con);
                                return true;
                            } else {
                            	// TODO (Jason) locks should be called here. 
                            	
                                //TODO(nelson): store username and secret then return REGISTER_SUCCESS to the client
                                userData.put(jsonObject.get("username").toString(), jsonObject.get("secret").toString());

                                response.put("command", "REGISTER_SUCCESS");
                                response.put("info", "register success for " + jsonObject.get("username"));

                                con.writeMsg(response.toJSONString());
                            }

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
                    if (clientConnections.containsKey(con)) clientConnections.remove(con); // TODO(Nelson) if doesn't contain, should send back INVALID MSG?
                    if (connections.contains(con)) connections.remove(con); // TODO(joanna) remove if our code that adds to clientConnections works fine.
                    return true;

                /** ACTIVITY OBJECT MESSAGES */
                case "ACTIVITY_MESSAGE":
                    // Check if username is "anonymous" OR matches logged in user
                    if (!clientConnections.containsKey(con)) return auth_failed(con, "User not logged in, cannot send Activity Message");

                    if (jsonObject.get("username") == null) return invalid_message(con, "Activity message missing username field");
                    if (jsonObject.get("activity") == null) return invalid_message(con, "Activity message missing activity field");

                    if (!jsonObject.get("username").toString().equals(clientConnections.get(con).username)) {
                        if (!jsonObject.get("username").toString().equals("anonymous")) { // for non-anonymous users
                            if (jsonObject.get("secret") == null) return invalid_message(con, "Activity message missing secret field");

                            if (!jsonObject.get("secret").toString().equals(clientConnections.get(con).secret)) {
                                return auth_failed(con, "User Secret in Activity Message does not match logged in user");
                            }
                        }
                    } else {
                        return auth_failed(con, "Username in Activity Message does not match logged in user");
                    }

                    String processedObj = process_activity_object(con, jsonObject.get("activity").toString());
                    if (processedObj == null) return true;

                    // Broadcast the activity object to all servers + all other clients 
                    response.put("command", "ACTIVITY_BROADCAST");
                    response.put("activity", processedObj);

                    for (Connection server:serverConnections) {
                        server.writeMsg(response.toJSONString());
                    }
                    for (Connection client:clientConnections.keySet()) {
                        if (client == con) continue;
                        client.writeMsg(response.toJSONString());
                    }
                    break;

                case "ACTIVITY_BROADCAST":
                    if (!serverConnections.contains(con)) return invalid_message(con,"ACTIVITY_BROADCAST message received from unauthenticated server");
                    if (jsonObject.get("activity") == null) return invalid_message(con, "ACTIVITY_BROADCAST message missing activity object");

                    jsonObject.remove("command");
                    jsonObject.remove("activity");
                    if (!jsonObject.isEmpty()) return invalid_message(con, "ACTIVITY_BROADCAST message has invalid fields");

                    for (Connection server:serverConnections) {
                        if (server == con) continue;
                        server.writeMsg(msg);
                    }
                    for (Connection client:clientConnections.keySet()) {
                        client.writeMsg(msg);
                    }
                    break;

                /** SERVER ANNOUNCEMENT MESSAGES */
                case "SERVER_ANNOUNCE":
                    if (!serverConnections.contains(con)) return invalid_message(con, "SERVER_ANNOUNCE message received from unauthenticated server");

                    if (jsonObject.get("hostname") == null) return invalid_message(con, "SERVER_ANNOUNCE message missing hostname field");
                    if (jsonObject.get("port") == null) return invalid_message(con, "SERVER_ANNOUNCE message missing port field");
                    if (jsonObject.get("id") == null) return invalid_message(con, "SERVER_ANNOUNCE message missing ID field");
                    if (jsonObject.get("load") == null) return invalid_message(con, "SERVER_ANNOUNCE message missing load field");

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
                
                /*
                 * Broadcast from a server to all other servers (only between servers), to indicate that a client is trying to
                 * register a username with a given secret.
                 */
                case "LOCK_REQUEST":
                	/*if it receives a LOCK_REQUEST from an 
                	 * unauthenticated server (the sender has not authenticated with the server secret). The connection is closed.
                	 */
                	if (!serverConnections.contains(con)) {
                		// If not in server connections, it means it is either client or unauthorized. 
                		return invalid_message(con, "LOCK_REQUEST sent by something that is not authenticated server");
                	}
                	
                	// Some defensive programming to ensure that username and secret were actually passed.
                	if (jsonObject.get("username") == null) {
                		return invalid_message(con, "LOCK_REQUEST received without any username");
                	}
                	if (jsonObject.get("secret") == null) {
                		return invalid_message(con, "LOCK_REQUEST received without any secret");
                	}
                	
                	String lockRequestUsername = jsonObject.get("username").toString();
                	String lockRequestSecret = jsonObject.get("secret").toString();
                	
                	// First, we broadcast lock_request to all servers. 
                	for (Connection server:serverConnections) {
                		if (server == con) continue;
                		server.writeMsg(msg); //TODO might not be full msg. 
                	}
                	
                	// Broadcast a LOCK_DENIED to all other servers, if username is already known to the server with a different secret. 
                	if (userData.containsKey(lockRequestUsername)) {
                		if (lockRequestSecret!=userData.get(lockRequestUsername)) {
                			// Code that broadcasts LOCK_DENIED
                			for (Connection server:serverConnections) { 
                                if (server == con) continue;
                                response.put("command", "LOCK_DENIED");
                                response.put("username", lockRequestUsername);
                                response.put("secret", lockRequestSecret);
                                server.writeMsg(response.toJSONString()); 
                            }
                		}
                	}
                	             	
                	/* Broadcast a LOCK_ALLOWED to all other servers (between servers only) if the username is not already known 
                	 * to the server. The server will record this username and secret pair in its local storage. 
                	 */
                	if (!userData.containsKey(lockRequestUsername)) {
                		userData.put(lockRequestUsername,  lockRequestSecret); // Add to local storage. 
                		// Code that broadcasts LOCK_ALLOWEd
                		for (Connection server:serverConnections) { 
                            if (server == con) continue;
                            server.writeMsg("LOCK_ALLOWED"); 
                            
                            response.put("command", "LOCK_ALLOWED");
                            response.put("username", lockRequestUsername);
                            response.put("secret", lockRequestSecret);
                            server.writeMsg(response.toJSONString()); 
                            	
                        }
                	}                	
                	
                	break;
                
                case "LOCK_DENIED":
                	/*if it receives a LOCK_DENIED from an 
                	 * unauthenticated server (the sender has not authenticated with the server secret). The connection is close
                	 */
                	if (!serverConnections.contains(con)) {
                		// If not in server connections, it means it is either client or unautharized. 
                		return invalid_message(con, "LOCK_DENIED sent by something that is not authenticated server");
                	}
                	
                	// Some defensive programming to ensure the jsonObject we received has a username and secret.
                	if (jsonObject.get("username") == null) {
                		return invalid_message(con, "LOCK_DENIED received with no username");
                	}
                	if (jsonObject.get("secret") == null) {
                		return invalid_message(con, "LOCK_DENIED received with no secret");
                	}
                	                	
                	// Getting username and secret that should be passed through.
                	String lockDeniedUsername= jsonObject.get("username").toString();
                	String lockDeniedSecret = jsonObject.get("secret").toString();
                	                	
                	// Remove username if secret matches the associated secret in its local storage. 
                	if(userData.containsKey(lockDeniedUsername)) {
                		if(userData.get(lockDeniedUsername) == lockDeniedSecret) {
                			userData.remove(lockDeniedUsername);
                		}
                	}
                	
                	
                	
                	break;
                
                case "LOCK_ALLOWED":
                	/*if it receives a LOCK_ALLOWED from an 
                	 * unauthenticated server (the sender has not authenticated with the server secret). The connection is close
                	 */
                	if (!serverConnections.contains(con)) {
                		// If not in server connections, it means it is either client or unautharized. 
                		return invalid_message(con, "LOCK_ALLOWED sent by something that is not authenticated server");
                	}
                	
                	// Some defensive programming to ensure the jsonObject we received has a username and secret.
                	if (jsonObject.get("username") == null) {
                		return invalid_message(con, "LOCK_DENIED received with no username");
                	}
                	if (jsonObject.get("secret") == null) {
                		return invalid_message(con, "LOCK_DENIED received with no secret");
                	}
                	

				default:
                    invalid_message(con, "Invalid command received, no specific information available"); 
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
            if (clientConnections.containsKey(con)) clientConnections.remove(con);
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
        for(Connection client:clientConnections.keySet()){
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


	/*
	 * Private Functions
	 */


}
