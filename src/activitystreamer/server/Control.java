package activitystreamer.server;

import java.io.Console;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.net.Socket;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import com.google.gson.Gson;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonArray;
import com.google.gson.JsonParser;
import com.google.gson.stream.JsonWriter;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import activitystreamer.util.Settings;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;



public class Control extends Thread {
	private static final Logger log = LogManager.getLogger();
	// Connections are just client connections, not server connections
	private static ArrayList<Connection> connections;  // can use .writeMsg() to send messages to that Connection
	private static boolean term=false;
	private static Listener listener;
	
	protected static Control control = null;
	
	public static Control getInstance() {
		if(control==null){
			control=new Control();
		} 
		return control;
	}
	
	public Control() {
		// initialize the connections array
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
		// TODO() do some error handling here for the messages
	    // return true;

        //TODO() check if is a client or server

		JSONObject jsonObject;
		JSONParser parser = new JSONParser();
        JSONObject response = new JSONObject();

        boolean valid = false;

        String jsonString;
		try {
			jsonObject = (JSONObject) parser.parse(msg);
		} catch (ParseException e) {
			log.error("Cannot parse JSON object: "+ e);
			return false; //TODO() maybe send back an error message?
		}

		if (jsonObject != null) {
		    if (jsonObject.get("command") == null) {
                log.error("Invalid message");
                //TODO(nelson): send back Invalid message

				response.put("command", "INVALID_MESSAGE");
				response.put("info", "JSON parse error while parsing message");

				con.writeMsg(response.toJSONString());
                return true;
            }

			String command = jsonObject.get("command").toString();

			switch (command) {
				case "AUTHENTICATE":
				    //TODO() check if secret is the same
                    if (jsonObject.get("secret") != null && jsonObject.get("secret").toString().equals(Settings.getSecret())) {
                        log.info("yay correct secret");
                    } else {
                        // TODO() send back AUTHENTICATION_FAILED message
                        return true;
                    }
					break;
                case "LOGIN":
                    //TODO(nelson): process username and secret on login
                    if (jsonObject.get("username") != null && jsonObject.get("secret") != null){

                        try {
                            Object obj  = parser.parse(new FileReader("D:/MIT/comp90015/distributed-systems-1/src/activitystreamer/data/user.json"));

                            JSONObject existingJson = (JSONObject) obj;
                            JSONArray jsonArray = (JSONArray) existingJson.get("users");

                            for (Object item : jsonArray) {
                                JSONObject pair = (JSONObject) item;

                                if ((pair.get("username").equals(jsonObject.get("username"))) && (pair.get("secret").equals(jsonObject.get("secret")))) {
                                    valid = true;
                                    break;
                                }
                            }

                            if (valid){
                                response.put("command", "LOGIN_SUCCESS");
                                response.put("info", "logged in as user " + jsonObject.get("username"));

                                con.writeMsg(response.toJSONString());
                            } else {
                                response.put("command", "LOGIN_FAILED");
                                response.put("info", "attempt to login with wrong secret");

                                con.writeMsg(response.toJSONString());
                                return true;
                            }
                        } catch (Exception e){
                            log.info(e);
                        }
                    }else {
						response.put("command", "INVALID_MESSAGE");
						response.put("info", "invalid username or secret");

						con.writeMsg(response.toJSONString());
						return true;
                    }

                    break;
                case "REGISTER":
                    //TODO(nelson): check if username already exist
                    log.info("Entered register");
                    if(jsonObject.get("username") != null){
                        try {
                            Object obj  = parser.parse(new FileReader("D:/MIT/comp90015/distributed-systems-1/src/activitystreamer/data/user.json"));

                            JSONObject existingJson = (JSONObject) obj;
                            JSONArray jsonArray = (JSONArray) existingJson.get("users");

                            for (Object item : jsonArray){
                                JSONObject pair = (JSONObject) item;

                                if ((pair.get("username").equals(jsonObject.get("username"))) && (pair.get("secret").equals(jsonObject.get("secret")))) {
                                    valid = true;
                                    break;
                                }
                            }

                            if (valid){
                                //TODO(nelson): send REGISTER_FAILED to the client

                                log.info("Username found");

                                response.put("command", "REGISTER_FAILED");
                                response.put("info", jsonObject.get("username") + " is already registered with the system");

                                con.writeMsg(response.toJSONString());

                                log.info("Message sent");

                                return true;
                            } else {
                                //TODO(nelson): store username and secret then return REGISTER_SUCCESS to the client

                                JSONObject newUser = new JSONObject();
                                newUser.put("username", jsonObject.get("username"));
                                newUser.put("secret", jsonObject.get("secret"));
                                jsonArray.add(newUser);

                                existingJson.put("users", jsonArray);
                                log.info(existingJson);

                                try(FileWriter file = new FileWriter("D:/MIT/comp90015/distributed-systems-1/src/activitystreamer/data/user.json")){

                                    file.write(existingJson.toJSONString());
                                    log.info("Register success");

                                    response.put("command", "REGISTER_SUCCESS");
                                    response.put("info", "register success for " + jsonObject.get("username"));

                                    con.writeMsg(response.toJSONString());

                                    log.info("Message sent");
                                }
                            }

                        } catch (Exception e){
                            log.info(e);
                            return true;
                        }
                    } else {
                        //TODO(nelson): send back INVALID_MESSAGE

						response.put("command", "INVALID_MESSAGE");
						response.put("info", "no username specified");

						con.writeMsg(response.toJSONString());
						return true;
                    }
                    break;
                case "LOGOUT":
                    return true;
				default:
                    // TODO() send back error message
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
