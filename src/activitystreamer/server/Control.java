package activitystreamer.server;

import java.io.IOException;
import java.net.Socket;
import java.util.*;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import activitystreamer.util.Settings;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;


public class Control extends Thread {
    /* Constants */
    private static final int LOAD_DIFF = 2;
    private static final String ANONYMOUS = "anonymous";

    private static String id = Settings.nextSecret();

    private static final Logger log = LogManager.getLogger();
    private static Listener listener;
    private static boolean term = false;

    private HashMap<Connection, ClientDetails> clientConnections;  // Client connections
    private HashSet<Connection> connections;  // All initial, unauthorized connections (client and server)

    HashMap<String, String> userData; // Local storage of registered users

    private static int lockAllowedReceived = 0; // To keep track of how many more lock_allowed we need.
    private static int lockDeniedReceived = 0; // To keep track of how many lock_denied we receive.
    private static boolean waitingForLockAllowed = false; //Toggled when lock_broadcast sent out. 

    private boolean waitingForServerAnnounce = false; //Toogled when we sent server_announce.
    private ServerDetails outgoingServer = null;
    private ServerDetails incomingServer = null; // if received as null, just set it as that connection
    private HashMap<String, ServerDetails> allServers; // map of all servers in DS
    // ServerDetails keeps track of prev and next so easier to recover in the event of server failure
    private int missedAnnounce = 0;
    private Set<String> loggedInClients = new HashSet<String>();

    // TODO: QUIT message should be implemented - called on close?
    // TODO: reconnection when quit / crash -- discovery + fix
    // TODO: reconnection discovery -- connectionClosed() called? Is this auto? Check this.


    protected static Control control = null;

    public static Control getInstance() {
        if (control == null) {
            control = new Control();
        }
        return control;
    }

    public Control() {
        // Initialize the connections arrays
        allServers = new HashMap<>();
        clientConnections = new HashMap<>();
        connections = new HashSet<>();

        userData = new HashMap<>();

        // Start a listener
        try {
            listener = new Listener();
        } catch (IOException e1) {
            log.fatal("failed to startup a listening thread: " + e1.toString());
            System.exit(-1);
        }

        // Connect to another server if remote hostname and port is supplied
        if (Settings.getRemoteHostname() != null) {
            initiateConnection(null, 0, null);
        } else {
            log.info("First server in system! Secret: " + Settings.getSecret());
        }
    }

    /**
     * Initiates connection with a server that is already in the distributed network and sends the appropriate
     * authentication messages to that server.
     */
    private void initiateConnection(String remoteHostname, int remotePort, String remoteId) {
        if (remoteHostname == null) {
            remoteHostname = Settings.getRemoteHostname();
            remotePort = Settings.getRemotePort();
        }

        try {
            log.info("Initiating connection to "+remoteHostname+":"+remotePort);

            Connection otherServer = outgoingConnection(new Socket(remoteHostname, remotePort));

            JSONObject msg = new JSONObject();
            msg.put("command", "AUTHENTICATE");
            msg.put("secret", Settings.getSecret());
            msg.put("id", id);
            msg.put("hostname", Settings.getLocalHostname());
            msg.put("port", Settings.getLocalPort());

            otherServer.writeMsg(msg.toJSONString());

            outgoingServer = new ServerDetails(remoteId, otherServer, remoteHostname, remotePort);
        } catch (IOException e) {
            log.error("Failed to connect to " + remoteHostname + ":" + String.valueOf(remotePort) + ", " + e);
            System.exit(-1);
        }
    }

    /**
     * Processes activity object by adding a authenticated_user field. Also checks that it's a valid JSON object.
     * Returns the processed activity object as a string. Returns null if something goes wrong and connection
     * should be closed.
     *
     * @param con              Connection the message was received from
     * @param jsonObjectString the JSON activity object formatted as a string
     */
    private String process_activity_object(Connection con, String jsonObjectString) {
        JSONObject jsonObject;
        JSONParser parser = new JSONParser();

        try {
            jsonObject = (JSONObject) parser.parse(jsonObjectString);
        } catch (ParseException e) {
            log.error("Cannot parse JSON object: " + e.toString());
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

        return jsonObject.toJSONString();
    }

    /**
     * Sends back a general message with the command INVALID_MESSAGE when there is anything incorrect
     * about the message that was received. A helpful informative message is usually included as well. Once the
     * message is sent, the connection is closed. This message can be sent to both clients and servers.
     *
     * @param con  Connection to send invalid message to
     * @param info Helpful message that explains what's wrong with the request
     */
    private boolean invalid_message(Connection con, String info) {
        JSONObject response = new JSONObject();
        response.put("command", "INVALID_MESSAGE");
        response.put("info", info);
        con.writeMsg(response.toJSONString());

        log.info("Invalid message received from " + con.getSocket().toString() + ": " + info);
        
        // Remove from any connections list
        if (connections.contains(con)) connections.remove(con);
        if (clientConnections.containsKey(con)) clientConnections.remove(con);
        // TODO: handle disconnected servers
        
        if (incomingServer.connection == con) {

        	
        	if (incomingServer.prevId != null) {
        		incomingServer = allServers.get(incomingServer.prevId);
        		
        	}
        	
        }
         

        return true; // Close connection
    }

    /**
     * Redirects client connection to another server to balance the servers' loads by sending a REDIRECT message
     * to the client. Server only redirects a client to another server that has at least some number of clients less
     * than the original.
     *
     * @param con  The client Connection to redirect
     */
    private boolean redirect(Connection con) {
        JSONObject response = new JSONObject();

        // Check if got other servers with at least 2 clients less than this server (incl new one)
        for (ServerDetails sd : allServers.values()) {

            if ((clientConnections.size() - sd.load) >= LOAD_DIFF) {
                log.info("Sending redirect message to: " + con.getSocket().toString());

                // Send REDIRECT message with new server's hostname and port number
                response.put("command", "REDIRECT");
                response.put("hostname", sd.hostname);
                response.put("port", sd.port);

                con.writeMsg(response.toJSONString());

                clientConnections.put(con, null);
                return true; // Close connection
            }
        }

        return false;
    }

    /**
     * Sends back a general message with the command AUTHENTICATION_FAIL when there is some issue with authentication.
     * A helpful informative message is usually included as well. Once the message is sent, the connection is closed.
     * This message can be sent to both clients and servers.
     *
     * @param con  Connection to send AUTHENTICATION_FAIL message to
     * @param info Helpful message that explains what's wrong with the authentication
     */
    private boolean auth_failed(Connection con, String info) {
    	log.debug("we are now sending a auth_fail");
        JSONObject response = new JSONObject();
        log.debug("INFO: "+ info);
        response.put("command", "AUTHENTICATION_FAIL");
        response.put("info", info);

        con.writeMsg(response.toJSONString());
        return true;
    }	 
    
    public void updateUserData(Connection con, HashMap<String,String> newUD) {
    	this.userData = newUD;
    	return;
    }

    /**
     * Processing incoming messages from the connection.
     * Return true if the connection should close.
     *
     * @param con Connection that sent message
     * @param msg Message received
     */
    
    public boolean process(Connection con, String msg) {

        JSONObject jsonObject;
        JSONParser parser = new JSONParser();
        JSONObject response = new JSONObject();

        try {
            jsonObject = (JSONObject) parser.parse(msg);
        } catch (ParseException e) {
            log.error("Cannot parse JSON object: " + e.toString());
            return invalid_message(con, "JSON parse error while parsing message");
        }

        if (jsonObject != null) {
            if (jsonObject.get("command") == null) {
                return invalid_message(con, "Message received did not contain a command");
            }

            String command = jsonObject.get("command").toString();

            if (clientConnections.containsKey(con) && clientConnections.get(con) == null) {
                if (!command.equals("LOGOUT")) return invalid_message(con, "Client should be logged out because redirected");
            }
       
            switch (command) {
                //======================================================================================================
                //                                       Server Joining Messages
                //======================================================================================================
                case "NEW_SERVER":
                    log.info("NEW_SERVER message received");

                    // Check validity of message
                    if (jsonObject.size() != 4) return invalid_message(con, "NEW_SERVER has invalid number of arguments");
                    if (jsonObject.get("new_hostname") == null) return invalid_message(con, "NEW_SERVER received without new_hostname");
                    if (jsonObject.get("new_port") == null) return invalid_message(con, "NEW_SERVER received without new_port");
                    if (jsonObject.get("new_id") == null) return invalid_message(con, "NEW_SERVER received without new_id");

                    // Situation: server B connects to server A (which is in the system) via AUTHENTICATE
                    // A sends NEW_SERVER with B's details to old incoming server (this server)
                    // once old incoming server gets NEW_SERVER from A with B's details
                    // old incoming server should start a new outgoing connection with B
                    // then, they should close connection with A (if > 2 servers in system)
                    boolean shouldClose = true;

                    if (incomingServer == null || outgoingServer == null ||
                            incomingServer.serverId == outgoingServer.serverId) {
                        // less than 2 servers in the system, shouldn't close con, still need it for incoming con
                        shouldClose = false;
                        log.debug("keep connection open");
                    }

                    // initiate new outgoing connection with server B
                    initiateConnection(jsonObject.get("new_hostname").toString(),
                            new Integer(jsonObject.get("new_port").toString()),
                            jsonObject.get("new_id").toString());

                    if (shouldClose) return true; // close old outgoing connection
                    break;
                case "REMOVE_SERVER":
                	// TODO here, we need to modify addServers and we need to send message long
                	// if this servers' outgoing is the dead one, we must change it to outgoing.outgoing. 
                	log.info("REMOVE_SERVER received");
                	JSONObject msgObj = new JSONObject(); 
            		msgObj.put("command", "REMOVE_SERVER");
                    outgoingServer.connection.writeMsg(msgObj.toString());
                	
                	break;

                case "SECOND_SERVER":
                    // Check authenticity of sender
                    if (!outgoingServer.connection.equals(con)) {
                        return invalid_message(con, "SECOND_SERVER received from unauthenticated server");
                    }
                    if (incomingServer != null) {
                        return invalid_message(con, "This server is NOT the second server of the system");
                    }

                    // Check validity of message
                    if (jsonObject.size() != 4) return invalid_message(con, "SECOND_SERVER has invalid number of arguments");
                    if (jsonObject.get("in_hostname") == null) return invalid_message(con, "SECOND_SERVER received without in_hostname");
                    if (jsonObject.get("in_port") == null) return invalid_message(con, "SECOND_SERVER received without in_port");
                    if (jsonObject.get("in_id") == null) return invalid_message(con, "SECOND_SERVER received without in_id");

                    // Message is just so this server knows it's the second in the system and can set its
                    // incomingServer to be the same as its outgoingServer
                    log.info("This server is the second server in the system");
                    incomingServer = new ServerDetails(
                            jsonObject.get("in_id").toString(),
                            con,
                            jsonObject.get("in_hostname").toString(),
                            new Integer(jsonObject.get("in_port").toString()));

                    outgoingServer.serverId = jsonObject.get("in_id").toString();
                    break;

                case "AUTHENTICATE":
                	log.info("AUTHENTICATE message received");
                    /*
                     * If it not in our list of current unauthorized connections, it is either a server that is already
                     * authenticated and trying to re-authenticate, or a client trying to authenticate as a server.
                     * In both cases, we return invalid_message.
                     */
                    if (!connections.contains(con)) {
                        if (outgoingServer.connection.equals(con) || incomingServer.connection.equals(con)) {
                            return invalid_message(con, "Server connection already authenticated");
                        } else {
                            return invalid_message(con, "Client connection trying to authenticate as a server");
                        }
                    }

                    // Handle invalid number of arguments
                    if (jsonObject.size() != 5) return invalid_message(con, "AUTHENTICATE has invalid number of arguments");

                    if (jsonObject.get("secret") == null) return invalid_message(con, "AUTHENTICATE did not provide secret");
                    if (jsonObject.get("hostname") == null) return invalid_message(con, "AUTHENTICATE did not provide hostname");
                    if (jsonObject.get("port") == null) return invalid_message(con, "AUTHENTICATE did not provide port");
                    if (jsonObject.get("id") == null) return invalid_message(con, "AUTHENTICATE did not provide server ID");
                    /*
                     * If it is in our list of current unauthorized connections and if it has a secret and it is the
                     * right secret, we add it as our incomingServer. Otherwise, we remove it from unauthorized
                     * connections and then return auth_failed to close the connection.
                     */
                    if (jsonObject.get("secret").toString().equals(Settings.getSecret())) {

                        connections.remove(con);

                        String newConHostname = jsonObject.get("hostname").toString();
                        String newConPort = jsonObject.get("port").toString();

                        // Only send NEW_SERVER to old incoming server when is this AUTHENTICATE message is
                        // sent by a new server in the system
                        if (incomingServer != null) {
                            JSONObject newServer = new JSONObject();
                            newServer.put("command", "NEW_SERVER");
                            newServer.put("new_hostname", newConHostname);
                            newServer.put("new_port", newConPort);
                            newServer.put("new_id", jsonObject.get("id").toString());

                            incomingServer.connection.writeMsg(newServer.toJSONString());
                            log.info("Send NEW_SERVER: "+newConHostname+":"+newConPort);
                        }

                        // Replace incomingServer with new incoming server
                        incomingServer = new ServerDetails(
                                jsonObject.get("id").toString(),
                                con,
                                newConHostname,
                                new Integer(newConPort));

                        if (outgoingServer == null) {
                            // This server is the first in the system, so the new server is the second and needs
                            // to set its incoming to the first server as well.
                            JSONObject newServer = new JSONObject();
                            newServer.put("command", "SECOND_SERVER");
                            newServer.put("in_hostname", Settings.getLocalHostname());
                            newServer.put("in_port", Settings.getLocalPort());
                            newServer.put("in_id", id);
                            con.writeMsg(newServer.toJSONString());

                            // Also the first needs to set its new outgoingServer as the second server.
                            outgoingServer = new ServerDetails(
                                    jsonObject.get("id").toString(),
                                    con,
                                    newConHostname,
                                    new Integer(newConPort));
                        }
                        log.info("Successful server authentication: " + con.getSocket().toString());

                        if (userData != null) {
                        	for(HashMap.Entry<String, String> entry : userData.entrySet()) {
                                JSONObject newUserAdd = new JSONObject();
                                newUserAdd.put("command", "NEW_USER");
                                newUserAdd.put("username", entry.getKey());
                                newUserAdd.put("secret", entry.getValue());
                                

                                con.writeMsg(newUserAdd.toJSONString());		
                        		log.debug("Key = " + entry.getKey() + ", Value = " + entry.getValue() + "was added...");
                        	}
                        }
                        // If code reaches here, everything went well. Send a SERVER_ANNOUNCE so that all servers can have updated
                        // prevID and nextID. 
                        JSONObject serverAnnounceJSON = new JSONObject();
                        serverAnnounceJSON.put("command", "SERVER_ANNOUNCE");
                        serverAnnounceJSON.put("id", id);
                        serverAnnounceJSON.put("load", clientConnections.size());
                        serverAnnounceJSON.put("hostname", Settings.getLocalHostname());
                        serverAnnounceJSON.put("port", Settings.getLocalPort());
                        serverAnnounceJSON.put("twoPrevId", incomingServer.serverId);
                        outgoingServer.connection.writeMsg(serverAnnounceJSON.toString());
                    } else {
                        log.info("Failed server authentication: " + con.getSocket().toString());
                        connections.remove(con);

                        return auth_failed(con, "the supplied secret is incorrect: " + jsonObject.get("secret"));
                    }
                    
                    break;

                case "AUTHENTICATION_FAIL":
                    log.info("AUTHENTICATION_FAIL message received, closing connection.");
                    return true; // close connection

                //======================================================================================================
                //                                        Client Login Messages
                //======================================================================================================
                case "LOGIN":
                    /*
                     * If connection not in unauthorized connections, it is either a server trying to login as client or
                     * a client that is already logged in. In both cases, we return invalid_message.
                     */
                    if (!connections.contains(con)) {
                        if (outgoingServer.connection.equals(con) || incomingServer.connection.equals(con)) {
                            return invalid_message(con, "Server connection trying to login as a client");
                        } else {
                            return invalid_message(con, "Client connection already logged in");
                        }
                    }

                    /*
                     * If connection is in unauthorized connections, we have to see if the username and secret is not
                     * null. If both not null, and it is a valid login attempt, we remove it from unauthorized
                     * connections and add it to clientConnections. The server follows up a LOGIN_SUCCESS message with
                     * a REDIRECT message, so we need to add the current connection to clientConnections,
                     * before we do any form of REDIRECT.
                     */
                    if (jsonObject.size() > 3) return invalid_message(con, "LOGIN has invalid number of arguments");

                    // For anonymous users
                    if (jsonObject.get("username") != null && jsonObject.get("username").toString().equals(ANONYMOUS)) {
                        response.put("command", "LOGIN_SUCCESS");
                        response.put("info", "logged in as an anonymous user");

                        con.writeMsg(response.toJSONString());

                        connections.remove(con);
                        clientConnections.put(con, new ClientDetails());

       
                        
                        
                        // Check if should redirect client
                        return redirect(con);
                    }

                    // For non-anonymous users
                    if (jsonObject.get("username") != null && jsonObject.get("secret") != null) {

                        String username = jsonObject.get("username").toString();
                        String secret = jsonObject.get("secret").toString();

                        // Checks if username is in local storage
                        if (userData.containsKey(username)) {

                            if (userData.get(username).equals(secret)) {
                                // username and secret match up -- send back LOGIN SUCCESS

                                response.put("command", "LOGIN_SUCCESS");
                                response.put("info", "logged in as user: " + username);

                                con.writeMsg(response.toJSONString());

                                connections.remove(con);
                                clientConnections.put(con, new ClientDetails(username, secret));

                                JSONObject loginListUpdate = new JSONObject();
                                loginListUpdate.put("command", "LOGIN_UPDATE");
                                loginListUpdate.put("username",  username);
                                loginListUpdate.put("id", id);
                                outgoingServer.connection.writeMsg(loginListUpdate.toJSONString());
                                
                                
                                
                                // Check if should redirect client
                                redirect(con);
                            } else {
                                // username and secret don't match up -- send back LOGIN FAILED and close connection
                                response.put("command", "LOGIN_FAILED");
                                response.put("info", "attempt to login with wrong secret");

                                con.writeMsg(response.toJSONString());

                                connections.remove(con);
                                return true;
                            }
                        } else {
                            // username not in local storage -- send back LOGIN FAILED and close connection
                            response.put("command", "LOGIN_FAILED");
                            response.put("info", "username, " + username + ", is not registered");

                            con.writeMsg(response.toJSONString());

                            connections.remove(con);
                            return true;
                        }
                    } else {
                        return invalid_message(con, "invalid username or secret on LOGIN");
                    }
                    break;
                
                case "LOGIN_UPDATE":
                	log.info("LOGIN_UPDATE received");
                	// TODO: Defensive programming
                	
                	// if it is the id is not the originator's id, then add the username to list of logged in clients
                	// then pass the message along.
                	if (!jsonObject.get("id").toString().equals(id)) {
                		loggedInClients.add(jsonObject.get("username").toString());
                		
                		JSONObject loginUpdatePass = new JSONObject();
                		loginUpdatePass.put("command", "LOGIN_UPDATE");
                		loginUpdatePass.put("username",  jsonObject.get("username").toString());
                		loginUpdatePass.put("id",  jsonObject.get("id").toString());
                		outgoingServer.connection.writeMsg(loginUpdatePass.toString());
                	}else {
                		loggedInClients.add(jsonObject.get("username").toString());
                	}
                	break;
                
                case "NEW_USER":
                	userData.put(jsonObject.get("username").toString(), jsonObject.get("secret").toString());
                	log.info("new_user registered");
                	break;
                	
                case "LOGOUT":
                    if (clientConnections.containsKey(con)) { 
                    	clientConnections.remove(con);
                    	JSONObject logoutUpdate = new JSONObject();
                    	logoutUpdate.put("command",  "LOGOUT_UPDATE");
                    	logoutUpdate.put("username",  "");
                    }
                    if (connections.contains(con)) invalid_message(con, "LOGOUT message sent by non client");
                    if (outgoingServer.connection.equals(con) || incomingServer.connection.equals(con)) {
                        invalid_message(con, "LOGOUT message sent by a server");
                    }
                    return true;


                //======================================================================================================
                //                                    Client Registration Messages
                //======================================================================================================
                case "REGISTER":
                	log.debug("REGISTER received");
                	Control.lockAllowedReceived = 0;
                    Control.lockDeniedReceived = 0;
                    if (!connections.contains(con)) { // Cannot be registered as a client
                        if (outgoingServer.connection.equals(con) || incomingServer.connection.equals(con)) {
                            return invalid_message(con, "Server connection trying to register as a client");
                        } else {
                            return invalid_message(con, "Client connection already logged in");
                        }
                    }

                    // Handle invalid number of arguments
                    if (jsonObject.size() != 3) return invalid_message(con, "Invalid number of arguments");

                    // Check that username and secret fields are defined
                    if (jsonObject.get("username") != null && jsonObject.get("secret") != null) {
                        String username = jsonObject.get("username").toString();
                        String secret = jsonObject.get("secret").toString();

                        // Checks if username is in local storage
                        if (userData.containsKey(username)) {

                            log.info("Username found");

                            response.put("command", "REGISTER_FAILED");
                            response.put("info", username + " already registered with system");

                            con.writeMsg(response.toJSONString());

                            log.info("Message sent");

                            connections.remove(con);
                            return true;
                        } else {
                            response.put("command", "LOCK_ALLOWED");
                            response.put("username", username);
                            response.put("secret", secret);
                            
                            // If we reach here, we need to do the lock checks. 
                            Control.waitingForLockAllowed = true;
                            log.info("we are waiting on LOCK_ALLOWED, registration attempt will begin");



                            this.outgoingServer.connection.writeMsg(response.toJSONString());
                        	log.info("LOCK_REQUEST sent");
                        	while(Control.waitingForLockAllowed == true) {
                        		/* While we are still waiting, we cannot progress, just be stuck in this loop. 
                        		*  When this particular server receives the LOCK_ALLOWED it needs, that part of 
                        		*  the code will flip the waiting boolean to false, and we will proceed.
                        		*/
                        	}
                            log.info("we received enough LOCK_ALLOWED, registration will proceed");

                            // Store username and secret then return REGISTER_SUCCESS to the client
                            userData.put(username, secret);

                            response.put("command", "REGISTER_SUCCESS");
                            response.put("info", "REGISTER success for " + username);
                            con.writeMsg(response.toJSONString());
                        	}

                    } else {
                        return invalid_message(con, "username/secret unspecified for REGISTER");
                    }
                    break;
                
                        

                //======================================================================================================
                //                                            Lock Messages
                //======================================================================================================
                case "LOCK_DENIED":
                	log.debug("LOCK_DENIED received");

                    if (!incomingServer.connection.equals(con)) {
                        return invalid_message(con, "LOCK_DENIED received from unauthenticated server");
                    }

                    // Handle invalid number of arguments
                    if (jsonObject.size() != 3) return invalid_message(con, "LOCK_DENIED has invalid number of arguments");

                    // Ensure the jsonObject received has a username and secret.
                    if (jsonObject.get("username") == null) {
                        return invalid_message(con, "LOCK_DENIED received with no username");
                    }
                    if (jsonObject.get("secret") == null) {
                        return invalid_message(con, "LOCK_DENIED received with no secret");
                    }
                    
                    // Getting username and secret that should be passed through.
                    String lockDeniedUsername = jsonObject.get("username").toString();
                    String lockDeniedSecret = jsonObject.get("secret").toString();
                    Control.lockDeniedReceived++;

                    // Remove username if secret matches the associated secret in its local storage.
                    if (userData.containsKey(lockDeniedUsername)) {
                        if (Objects.equals(userData.get(lockDeniedUsername), lockDeniedSecret)) {
                            userData.remove(lockDeniedUsername);
                        }
                    }

                    // Broadcasts LOCK_ALLOWED to all other servers.
//                    for (Connection server : serverConnections) {
//                        if (server == con) continue;
//                        response.put("command", "LOCK_DENIED");
//                        response.put("username", lockDeniedUsername);
//                        response.put("secret", lockDeniedSecret);
//                        server.writeMsg(response.toJSONString());
//                    }

                    break;

                case "LOCK_ALLOWED":
                	// If we receive a LOCK_ALLOWED message, run some defensive programming checks. 
                    log.debug("LOCK_ALLOWED received");
                	// Checks if server received a LOCK_ALLOWED from an unauthenticated server
                    if (!incomingServer.connection.equals(con)) {
                        return invalid_message(con, "LOCK_ALLOWED received from unauthenticated server");
                    }

                    // Handle invalid number of arguments
                    if (jsonObject.size() != 3) return invalid_message(con, "LOCK_ALLOWED has invalid number of arguments");

                    // Ensure the jsonObject we received has a username and secret.
                    if (jsonObject.get("username") == null) {
                        return invalid_message(con, "LOCK_ALLOWED received with no username");
                    }
                    if (jsonObject.get("secret") == null) {
                        return invalid_message(con, "LOCK_ALLOWED received with no secret");
                    }
                    
                    // If code reaches here, it means all defensive checks passed. 
                    // Now we actually process the message. 
                    
                    // Getting username and secret that should be passed through.
                    String lockAllowedUsername = jsonObject.get("username").toString();
                    String lockAllowedSecret = jsonObject.get("secret").toString();
                    if (userData.containsKey(lockAllowedUsername)) {
                        if (!Objects.equals(lockAllowedSecret, userData.get(lockAllowedUsername))) {
                        	// lock denied
                        	response.put("command", "LOCK_DENIED");
                        	response.put("username", lockAllowedUsername);
                        	response.put("secret", lockAllowedSecret);
                        	this.outgoingServer.connection.writeMsg(response.toJSONString());
                        }
                    } else { 
                    	// Conditions for sending LOCK_DENIED not fulfilled..
                    	
                        
                    	// Add username-secret pair to local storage.
                        userData.put(lockAllowedUsername, lockAllowedSecret);
                        log.info("user data added for client attempting to register");
                        // If we are not the server waiting, then we need to pass the message on.
                        if(!this.waitingForLockAllowed) {
                        	response.put("command", "LOCK_ALLOWED");
                        	response.put("username", lockAllowedUsername);
                        	response.put("secret", lockAllowedSecret);
                        	this.outgoingServer.connection.writeMsg(response.toJSONString());
                        }else {
                        	// If we are the server waiting, we can stop waiting now. 
                        	this.waitingForLockAllowed = false;
                        }
                    }

                    
                    break;
                //======================================================================================================
                //                                     Activity Object Messages
                //======================================================================================================
                case "ACTIVITY_MESSAGE":
                	log.debug("ACTIVITY_MESSAGE was received");
                    // Check if username is ANONYMOUS OR matches logged in user
                    if (!clientConnections.containsKey(con))
                        return auth_failed(con, "User not logged in, cannot send Activity Message");

                    // Handle invalid number of arguments
                    if (jsonObject.size() > 4) return invalid_message(con, "ACTIVITY_MESSAGE has invalid number of arguments");

                    if (jsonObject.get("username") == null)
                        return invalid_message(con, "Activity message missing username field");
                    if (jsonObject.get("activity") == null)
                        return invalid_message(con, "Activity message missing activity field");

                    String username = jsonObject.get("username").toString();

                    if (username.equals(clientConnections.get(con).username)) {
                        if (!username.equals(ANONYMOUS)) { // for non-anonymous users
                            if (jsonObject.get("secret") == null)
                                return invalid_message(con, "Activity message missing secret field");

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

                    for (Connection client : clientConnections.keySet()) {
                        if (client == con) continue;
                        client.writeMsg(response.toJSONString());
                    }

                    // ensure that ACTIVITY_BROADCAST doesn't loop infinitely
                    response.put("sender_id", id);
                    outgoingServer.connection.writeMsg(response.toJSONString());

                    break;

                case "ACTIVITY_BROADCAST":
                	log.debug("ACTIVITY_BROADCAST was received");

                	// Check authenticity of sender
                    if (!incomingServer.connection.equals(con)) {
                        return invalid_message(con, "ACTIVITY_BROADCAST received from unauthenticated server");
                    }

                    // Check validity of message
                    if (jsonObject.get("activity") == null) {
                        return invalid_message(con, "ACTIVITY_BROADCAST message missing activity object");
                    }
                    if (jsonObject.size() != 3) return invalid_message(con, "ACTIVITY_BROADCAST has invalid number of arguments");

                    // Check if this server was the original sender -- no need send again
                    // Already sent to the original server's clients in ACTIVITY_MESSAGE
                    if (jsonObject.get("sender_id").toString().equals(id)) {
                        break;
                    }

                    outgoingServer.connection.writeMsg(msg);

                    jsonObject.remove("sender_id"); // remove server details before sending to clients

                    for (Connection client : clientConnections.keySet()) {
                        client.writeMsg(jsonObject.toJSONString());
                    }
                    break;

                //======================================================================================================
                //                                    Server Announcement Messages
                //======================================================================================================
                case "SET_TWO_FRONT":
                	//TODO uncomment
                	//log.info("SET_TWO_FRONT received");
                	//defensive programming
                	if (jsonObject.get("originalId").toString().equals(id)) {
                		incomingServer.connection.writeMsg(jsonObject.toString());
                	}else {
                		outgoingServer.nextId = jsonObject.get("originalId").toString();
                	}
                	break;
                	
                case "SERVER_ANNOUNCE":
                	//log.info("we received server_announce");
                	// TODO here uncomment
                	
                	/*
                    // Check if authenticated server + message is going in right direction
                    if (!incomingServer.connection.equals(con)) {
                        return invalid_message(con, "SERVER_ANNOUNCE received from unauthenticated server");
                    }
                    */

                    if (jsonObject.get("hostname") == null)
                        return invalid_message(con, "SERVER_ANNOUNCE message missing hostname field");
                    if (jsonObject.get("port") == null)
                        return invalid_message(con, "SERVER_ANNOUNCE message missing port field");
                    if (jsonObject.get("id") == null)
                        return invalid_message(con, "SERVER_ANNOUNCE message missing ID field");
                    if (jsonObject.get("load") == null)
                        return invalid_message(con, "SERVER_ANNOUNCE message missing load field");

                    // If code reaches here, it means we are receiving a proper server_announce that 
                    // We now want to update and make sure that our prevID and nextID are correct. 
                    waitingForServerAnnounce = false; 
                    outgoingServer.prevId = id;
                    incomingServer.nextId = id;
                    if (jsonObject.get("twoPrevId") != null) // can be null if only one server in system. 
                    	incomingServer.prevId = jsonObject.get("twoPrevId").toString();
                    /*
                    if (outgoingServer.prevId == null) {
                    	log.info("failuretodo1");
                    }
                    
                    if (incomingServer.nextId == null) {
                    	log.info("failuretodo2");
                    }
                    */
                    // Code that we propogate two servers back, to update the outgoing.outgoing.connection
                    // of the server two servers back. This is part of our failure handling, if one server crashes,
                    // this code is part of the solution that makes the former incoming and former outgoing directly connect.
                    JSONObject updateTwoFront = new JSONObject();
                    updateTwoFront.put("command", "SET_TWO_FRONT");
                    updateTwoFront.put("originalId",  id);
                    con.writeMsg(updateTwoFront.toString());
                    
                    
                    
                    if (jsonObject.get("id").toString().equals(id)) {
                        // TODO uncomment
                    	//log.info("SERVER_ANNOUNCE has gone full circle");
                        // went full circle already, no need send again
                        break;
                    }
                    
                                  
                     
                    
                    
                    String conId = jsonObject.get("id").toString();

                    String prevId = (jsonObject.get("prev_id") != null) ? jsonObject.get("prev_id").toString() : null;
                    String nextId = (jsonObject.get("next_id") != null) ? jsonObject.get("next_id").toString() : null;

                    ServerDetails sd = new ServerDetails(
                            conId,
                            prevId,
                            nextId,
                            jsonObject.get("hostname").toString(),
                            new Integer(jsonObject.get("port").toString()),
                            new Integer(jsonObject.get("load").toString()));

                    // Updates client load in allServers
                    allServers.put(conId, sd);

                    // Forward this message to outgoingServer only
                    outgoingServer.connection.writeMsg(msg);
                    break;

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
    public synchronized void connectionClosed(Connection con) {
        if (!term) {
            if (connections.contains(con)) connections.remove(con);
            if (clientConnections.containsKey(con)) clientConnections.remove(con);
// TODO: handle servers closing
// if (serverConnections.contains(con)) serverConnections.remove(con);
        }
    }

    /*
     * A new incoming connection has been established, and a reference is returned to it
     */
    public synchronized Connection incomingConnection(Socket s) throws IOException {
        log.debug("incoming connection: " + Settings.socketAddress(s).toString());
        Connection c = new Connection(s);

        connections.add(c);
        return c;
    }

    /*
     * A new outgoing connection has been established, and a reference is returned to it
     */
    public synchronized Connection outgoingConnection(Socket s) throws IOException {
        log.debug("outgoing connection: " + Settings.socketAddress(s).toString());
        Connection c = new Connection(s);

        return c;
    }

    @Override
    public void run() {
        log.info("using activity interval of " + String.valueOf(Settings.getActivityInterval()) + " milliseconds");
        while (!term) {
            // do something with 5 second intervals in between
            try {
                Thread.sleep(Settings.getActivityInterval());
            } catch (InterruptedException e) {
                log.info("received an interrupt, system is shutting down");
                break;
            }
            if (!term) {
                //TODO uncomment
            	//log.debug("doing activity from: " + Settings.getLocalHostname() + ":" +
                //        String.valueOf(Settings.getLocalPort()));
                term = doActivity();
            }

        }
        // clean up by closing all connections
        log.info("closing " + String.valueOf(connections.size()) + " connection(s)");
        for (Connection connection : connections) {
            connection.closeCon();
        }
        for (Connection client : clientConnections.keySet()) {
            client.closeCon();
        }

        // TODO: handle servers closing
//        for (Connection server : serverConnections) {
//            server.closeCon();
//        }

        listener.setTerm(true);
    }

    public boolean doActivity() {
    	/*log.info("list of logged in guys");
    	Iterator iter = loggedInClients.iterator();
    	while (iter.hasNext()) {
    	    log.info(iter.next());
    	}*/
    	/*
    	//TODO delte
    	if(incomingServer!= null) {
    		log.info("random info1" + incomingServer.nextId);
    		log.info("random info2" + incomingServer.prevId);
    	}
    	if(outgoingServer!= null) {
    		log.info("random info3" + outgoingServer.nextId);
    		log.info("random info4" + outgoingServer.prevId);
    	}
    	*/
    	// TODO here
    	if(waitingForServerAnnounce && outgoingServer != null) {
    		missedAnnounce+=1;
    	}
    	if (missedAnnounce>1) {
    		log.info("incoming server has died, action needs to be taken.");
    		JSONObject msgObj = new JSONObject();
    		//server has died, take action.
    		incomingServer = allServers.get(incomingServer.prevId);
    		msgObj.put("command", "REMOVE_SERVER");
            outgoingServer.connection.writeMsg(msgObj.toString());

    	}
    	
    	// if we are sending server_announce, we are waiting for incoming to send server_announce to us.
    	
    	this.waitingForServerAnnounce = true; 
    	
        JSONObject msgObj = new JSONObject();

        msgObj.put("command", "SERVER_ANNOUNCE");
        msgObj.put("id", id);
        msgObj.put("load", clientConnections.size());
        msgObj.put("hostname", Settings.getLocalHostname());
        msgObj.put("port", Settings.getLocalPort());
        

        if (incomingServer != null) {
        	msgObj.put("prev_id", incomingServer.serverId);
        	msgObj.put("twoPrevId", incomingServer.serverId);
        }

        if (outgoingServer != null) {
            msgObj.put("next_id", outgoingServer.serverId);
            outgoingServer.connection.writeMsg(msgObj.toString());
            // Uncomment TODO
            log.info("outgoingServer: "+outgoingServer.hostname+":"+outgoingServer.port);
        }
        if (outgoingServer == null) {
        	log.debug("in doActivity, we cannot find outgoing");
        }

        // TODO uncomment
        if (incomingServer != null) log.info("incomingServer: "+incomingServer.hostname+":"+incomingServer.port);
        return false;
    }

    public final void setTerm(boolean t) {
        term = t;
    }
}
