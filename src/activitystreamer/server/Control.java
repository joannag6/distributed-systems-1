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

    private HashMap<String, ServerDetails> allServers;  // All servers in DS (server_id, server_details)
    private HashSet<Connection> serverConnections;  // Server connections
    private HashMap<Connection, ClientDetails> clientConnections;  // Client connections
    private HashSet<Connection> connections;  // All initial, unauthorized connections (client and server)

    private HashMap<String, String> userData; // Local storage of registered users

    private static int lockAllowedReceived = 0; // To keep track of how many more lock_allowed we need.
    private static int lockDeniedReceived = 0; // To keep track of how many lock_denied we receive.

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
        serverConnections = new HashSet<>();
        clientConnections = new HashMap<>();
        connections = new HashSet<>();

        userData = new HashMap<>();

        // Start a listener
        try {
            listener = new Listener();
        } catch (IOException e1) {
            log.fatal("failed to startup a listening thread: " + e1);
            System.exit(-1);
        }

        // Connect to another server if remote hostname and port is supplied
        if (Settings.getRemoteHostname() != null) {
            initiateConnection();
        } else {
            log.info("First server in system! Secret: " + Settings.getSecret());
        }
    }

    /**
     * Initiates connection with a server that is already in the distributed network and sends the appropriate
     * authentication messages to that server.
     */
    private void initiateConnection() {
        String remoteHostname = Settings.getRemoteHostname();
        int remotePort = Settings.getRemotePort();

        try {
            Connection otherServer = outgoingConnection(new Socket(remoteHostname, remotePort));

            JSONObject msg = new JSONObject();
            msg.put("command", "AUTHENTICATE");
            msg.put("secret", Settings.getSecret());

            otherServer.writeMsg(msg.toJSONString());
        } catch (IOException e) {
            log.error("Failed to connect to " + remoteHostname + ":" + remotePort + ", " + e);
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
            log.error("Cannot parse JSON object: " + e);
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

        log.info("Invalid message: " + info); //TODO, unsure if we need this to be here, but a log message would be helpful.

        // Remove from any connections list
        if (connections.contains(con)) connections.remove(con);
        if (clientConnections.containsKey(con)) clientConnections.remove(con);
        if (serverConnections.contains(con)) serverConnections.remove(con);

        return true; // Ensure connection always closed
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
        for (String id : allServers.keySet()) {
            ServerDetails sd = allServers.get(id);

            if ((clientConnections.size() - sd.load) >= LOAD_DIFF) {
                log.info("Sending redirect message to: " + con.getSocket());

                response.clear();

                // Send REDIRECT message with new server's hostname and port number
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

    /**
     * Sends back a general message with the command AUTHENTICATION_FAIL when there is some issue with authentication.
     * A helpful informative message is usually included as well. Once the message is sent, the connection is closed.
     * This message can be sent to both clients and servers.
     *
     * @param con  Connection to send AUTHENTICATION_FAIL message to
     * @param info Helpful message that explains what's wrong with the authentication
     */
    private boolean auth_failed(Connection con, String info) {
        JSONObject response = new JSONObject();

        response.put("command", "AUTHENTICATION_FAIL");
        response.put("info", info);

        con.writeMsg(response.toJSONString());
        return true;
    }

    /**
     * Processing incoming messages from the connection.
     * Return true if the connection should close.
     *
     * @param con Connection that sent message
     * @param msg Message received
     */
    public synchronized boolean process(Connection con, String msg) {

        JSONObject jsonObject;
        JSONParser parser = new JSONParser();
        JSONObject response = new JSONObject();

        try {
            jsonObject = (JSONObject) parser.parse(msg);
        } catch (ParseException e) {
            log.error("Cannot parse JSON object: " + e);
            return invalid_message(con, "JSON parse error while parsing message");
        }

        if (jsonObject != null) {
            if (jsonObject.get("command") == null) {
                return invalid_message(con, "Message received did not contain a command");
            }

            String command = jsonObject.get("command").toString();

            switch (command) {

                //======================================================================================================
                //                                   Server Authentication Messages
                //======================================================================================================
                case "AUTHENTICATE":
                    /*
                     * If it not in our list of current unauthorized connections, it is either a server that is already
                     * authenticated and trying to re-authenticate, or a client trying to authenticate as a server.
                     * In both cases, we return invalid_message.
                     */
                    if (!connections.contains(con)) {
                        if (serverConnections.contains(con)) {
                            return invalid_message(con, "Server connection already authenticated");
                        } else {
                            return invalid_message(con, "Client connection trying to authenticate as a server");
                        }
                    }

                    // Handle invalid number of arguments
                    if (jsonObject.size() != 2) return invalid_message(con, "AUTHENTICATE has invalid number of arguments");

                    /*
                     * If it is in our list of current unauthorized connections and if it has a secret and it is the
                     * right secret, we add it to our server list. Otherwise, we remove it from unauthorized
                     * connections and then return auth_failed to close the connection.
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
                    log.info("Something wrong with authentication, closing connection.");
                    return true; // close connection

                //======================================================================================================
                //                                        Client Login Messages
                //======================================================================================================
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
                     * If connection is in unauthorized connections, we have to see if the username and secret is not
                     * null. If both not null, and it is a valid login attempt, we remove it from unauthorized
                     * connections and add it to clientConnections. The server follows up a LOGIN_SUCCESS message with
                     * a REDIRECT message, so we need to add the current connection to clientConnections,
                     * before we do any form of REDIRECT.
                     */
                    if (jsonObject.size() != 3) return invalid_message(con, "LOGIN has invalid number of arguments");

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

                case "LOGOUT":
                    if (clientConnections.containsKey(con)) clientConnections.remove(con);
                    if (connections.contains(con)) invalid_message(con, "LOGOUT message sent by non client");
                    if (serverConnections.contains(con)) invalid_message(con, "LOGOUT message sent by a server");
                    return true;

                //======================================================================================================
                //                                    Client Registration Messages
                //======================================================================================================
                case "REGISTER":
                    Control.lockAllowedReceived = 0;
                    Control.lockDeniedReceived = 0;
                    if (!connections.contains(con)) { // Cannot be authenticated
                        if (serverConnections.contains(con)) {
                            return invalid_message(con, "Server connection trying to register as a client");
                        } else {
                            return invalid_message(con, "Client connection already logged in");
                        }
                    }

                    // Handle invalid number of arguments
                    if (jsonObject.size() != 3) return invalid_message(con, "Invalid number of arguments");

                    log.info("Starting registration process");


                    // Check that username and secret fields are defines
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
                            // First, we broadcast lock_request to all servers.
                            response.put("command", "LOCK_REQUEST");
                            response.put("username", username);
                            response.put("secret", secret);

                            for (Connection server : serverConnections) {
                                if (server == con) continue;
                                server.writeMsg(response.toJSONString());
                            }
                            int lockAllowedNeeded = allServers.size()-1; // number of servers in system - itself

                            /* Now we wait for enough number of LOCK_ALLOWED to be broadcasted back.
                             * Current specs do not allow us to know who is broadcasting back, in this situation.
                             */
                            while (Control.lockAllowedReceived < lockAllowedNeeded) {
                                // If we receive any LOCK_DENIED, break
                                if (Control.lockDeniedReceived > 0) {
                                    Control.lockAllowedReceived = 0;
                                    Control.lockDeniedReceived = 0;
                                    log.info("lock denied received during registration of: " + username);

                                    response.put("command", "REGISTER_FAILED");
                                    response.put("info", "lock denied received during registration of: " + username);

                                    con.writeMsg(response.toJSONString());

                                    connections.remove(con);
                                    return true;
                                }

                            }

                            log.info("reached here yay");

                            // If code reaches here, it means we received the right amount of lock_allowed.
                            // Reset it for when this server might receive another registration.
                            Control.lockAllowedReceived = 0;
                            Control.lockDeniedReceived = 0;

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
                case "LOCK_REQUEST":
                    // Checks if server received a LOCK_REQUEST from an unauthenticated server
                    if (!serverConnections.contains(con)) {
                        return invalid_message(con, "LOCK_REQUEST sent by something that is not authenticated server");
                    }

                    // Handle invalid number of arguments
                    if (jsonObject.size() != 3) return invalid_message(con, "LOCK_REQUEST has invalid number of arguments");

                    // Ensure that username and secret are included in the message received.
                    if (jsonObject.get("username") == null) {
                        return invalid_message(con, "LOCK_REQUEST received without any username");
                    }
                    if (jsonObject.get("secret") == null) {
                        return invalid_message(con, "LOCK_REQUEST received without any secret");
                    }

                    String lockRequestUsername = jsonObject.get("username").toString();
                    String lockRequestSecret = jsonObject.get("secret").toString();

                    // First, we broadcast lock_request to all servers.
                    for (Connection server : serverConnections) {
                        if (server == con) continue;
                        server.writeMsg(msg); //TODO (Jason) might not be full msg.
                    }

                    // Broadcast a LOCK_DENIED to all other servers, if username is already known to the server with a different secret.
                    if (userData.containsKey(lockRequestUsername)) {
                        if (lockRequestSecret != userData.get(lockRequestUsername)) { // TODO(Jason) what if secret is the same? - should still send back LOCK_DENIED

                            // Broadcasts LOCK_DENIED to all other servers.
                            for (Connection server : serverConnections) {
                                response.put("command", "LOCK_DENIED");
                                response.put("username", lockRequestUsername);
                                response.put("secret", lockRequestSecret);
                                server.writeMsg(response.toJSONString());
                            }
                        }
                    } else {
                        // Add username-secret pair to local storage.
                        userData.put(lockRequestUsername, lockRequestSecret);

                        log.info("lock req allowed yo");

                        // Broadcasts LOCK_ALLOWED to all other servers.
                        for (Connection server : serverConnections) {
                            response.put("command", "LOCK_ALLOWED");
                            response.put("username", lockRequestUsername);
                            response.put("secret", lockRequestSecret);
                            server.writeMsg(response.toJSONString());
                        }
                    }
                    break;

                case "LOCK_DENIED":
                    // Checks if server received a LOCK_DENIED from an unauthenticated server
                    if (!serverConnections.contains(con)) {
                        return invalid_message(con, "LOCK_DENIED sent by something that is not authenticated server");
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
                        if (userData.get(lockDeniedUsername) == lockDeniedSecret) {
                            userData.remove(lockDeniedUsername);
                        }
                    }

                    // TODO() send to other servers?
                    // Broadcasts LOCK_ALLOWED to all other servers.
                    for (Connection server : serverConnections) {
                        if (server == con) continue;
                        response.put("command", "LOCK_DENIED");
                        response.put("username", lockDeniedUsername);
                        response.put("secret", lockDeniedSecret);
                        server.writeMsg(response.toJSONString());
                    }

                    break;

                case "LOCK_ALLOWED":
                    // Checks if server received a LOCK_ALLOWED from an unauthenticated server
                    if (!serverConnections.contains(con)) {
                        return invalid_message(con, "LOCK_ALLOWED sent by something that is not authenticated server");
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
                    // Getting username and secret that should be passed through.
                    String lockAllowedUsername = jsonObject.get("username").toString();
                    String lockAllowedSecret = jsonObject.get("secret").toString();
                    Control.lockAllowedReceived++;
                    for (Connection server : serverConnections) {
                        if (server == con) continue;
                        response.put("command", "LOCK_ALLOWED");
                        response.put("username", lockAllowedUsername);
                        response.put("secret", lockAllowedSecret);
                        server.writeMsg(response.toJSONString());
                    }

                //======================================================================================================
                //                                     Activity Object Messages
                //======================================================================================================
                case "ACTIVITY_MESSAGE":
                    // Check if username is ANONYMOUS OR matches logged in user
                    if (!clientConnections.containsKey(con))
                        return auth_failed(con, "User not logged in, cannot send Activity Message");

                    // Handle invalid number of arguments
                    if (jsonObject.size() != 4) return invalid_message(con, "ACTIVITY_MESSAGE has invalid number of arguments");

                    if (jsonObject.get("username") == null)
                        return invalid_message(con, "Activity message missing username field");
                    if (jsonObject.get("activity") == null)
                        return invalid_message(con, "Activity message missing activity field");

                    if (!jsonObject.get("username").toString().equals(clientConnections.get(con).username)) {
                        if (!jsonObject.get("username").toString().equals(ANONYMOUS)) { // for non-anonymous users
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

                    for (Connection server : serverConnections) {
                        server.writeMsg(response.toJSONString());
                    }
                    for (Connection client : clientConnections.keySet()) {
                        if (client == con) continue;
                        client.writeMsg(response.toJSONString());
                    }
                    break;

                case "ACTIVITY_BROADCAST":
                    if (!serverConnections.contains(con))
                        return invalid_message(con, "ACTIVITY_BROADCAST message received from unauthenticated server");
                    if (jsonObject.get("activity") == null)
                        return invalid_message(con, "ACTIVITY_BROADCAST message missing activity object");

                    jsonObject.remove("command");
                    jsonObject.remove("activity");
                    if (!jsonObject.isEmpty())
                        return invalid_message(con, "ACTIVITY_BROADCAST message has invalid fields");

                    for (Connection server : serverConnections) {
                        if (server == con) continue;
                        server.writeMsg(msg);
                    }
                    for (Connection client : clientConnections.keySet()) {
                        client.writeMsg(msg);
                    }
                    break;

                //======================================================================================================
                //                                    Server Announcement Messages
                //======================================================================================================
                case "SERVER_ANNOUNCE":
                    if (!serverConnections.contains(con))
                        return invalid_message(con, "SERVER_ANNOUNCE message received from unauthenticated server");

                    // Handle invalid number of arguments
                    if (jsonObject.size() != 5) return invalid_message(con, "SERVER_ANNOUNCE has invalid number of arguments");

                    if (jsonObject.get("hostname") == null)
                        return invalid_message(con, "SERVER_ANNOUNCE message missing hostname field");
                    if (jsonObject.get("port") == null)
                        return invalid_message(con, "SERVER_ANNOUNCE message missing port field");
                    if (jsonObject.get("id") == null)
                        return invalid_message(con, "SERVER_ANNOUNCE message missing ID field");
                    if (jsonObject.get("load") == null)
                        return invalid_message(con, "SERVER_ANNOUNCE message missing load field");

                    String conId = jsonObject.get("id").toString();

                    ServerDetails sd = new ServerDetails(
                            conId,
                            jsonObject.get("hostname").toString(),
                            new Integer(jsonObject.get("port").toString()),
                            new Integer(jsonObject.get("load").toString()));

                    // Updates client load in ServerConnections
                    allServers.put(conId, sd);

                    // Forward this message to every other connected server except this connection
                    for (Connection server : serverConnections) {
                        if (server == con) continue;
                        server.writeMsg(msg);
                    }
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
            if (serverConnections.contains(con)) serverConnections.remove(con);
        }
    }

    /*
     * A new incoming connection has been established, and a reference is returned to it
     */
    public synchronized Connection incomingConnection(Socket s) throws IOException {
        log.debug("incoming connection: " + Settings.socketAddress(s));
        Connection c = new Connection(s);

        connections.add(c);
        return c;
    }

    /*
     * A new outgoing connection has been established, and a reference is returned to it
     */
    public synchronized Connection outgoingConnection(Socket s) throws IOException {
        log.debug("outgoing connection: " + Settings.socketAddress(s));
        Connection c = new Connection(s);

        serverConnections.add(c); // Outgoing connection is always a server
        return c;
    }

    @Override
    public void run() {
        log.info("using activity interval of " + Settings.getActivityInterval() + " milliseconds");
        while (!term) {
            // do something with 5 second intervals in between
            try {
                Thread.sleep(Settings.getActivityInterval());
            } catch (InterruptedException e) {
                log.info("received an interrupt, system is shutting down");
                break;
            }
            if (!term) {
                log.debug("doing activity from: " + Settings.getLocalPort());
                term = doActivity();
            }

        }
        log.info("closing " + connections.size() + " connection(s)");
        // clean up
        for (Connection connection : connections) {
            connection.closeCon();
        }
        for (Connection client : clientConnections.keySet()) {
            client.closeCon();
        }
        for (Connection server : serverConnections) {
            server.closeCon();
        }

        listener.setTerm(true);
    }

    public boolean doActivity() {
        JSONObject msgObj = new JSONObject();

        msgObj.put("command", "SERVER_ANNOUNCE");
        msgObj.put("id", id);
        msgObj.put("load", clientConnections.size());
        msgObj.put("hostname", Settings.getLocalHostname());
        msgObj.put("port", Settings.getLocalPort());

        for (Connection server : serverConnections) {
            server.writeMsg(msgObj.toString());
        }
        return false;
    }

    public final void setTerm(boolean t) {
        term = t;
    }
}
