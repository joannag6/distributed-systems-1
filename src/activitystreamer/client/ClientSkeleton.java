package activitystreamer.client;

import java.awt.event.WindowAdapter;
import java.awt.event.WindowEvent;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.Socket;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.json.simple.JSONObject;

import activitystreamer.util.Settings;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

public class ClientSkeleton extends Thread {
    private static final Logger log = LogManager.getLogger();
    private static ClientSkeleton clientSolution;
    private TextFrame textFrame;

    private Socket clientSocket = null;

    private PrintWriter out = null;
    private BufferedReader in = null;
    private boolean term = false;
    private boolean needRegister = true;
    private boolean needLogin = true;
    private boolean guiOpened = false;

    private String username = Settings.getUsername();
    private String secret = Settings.getSecret();

    public static ClientSkeleton getInstance() {
        if(clientSolution==null){
            clientSolution = new ClientSkeleton();
        }
        return clientSolution;
    }

    private ClientSkeleton() {
        start();
        openCon(Settings.getRemoteHostname(), Settings.getRemotePort());
    }

    private void openGUI() {
        textFrame = new TextFrame();
        textFrame.addWindowListener(new WindowAdapter(){
            public void windowClosing(WindowEvent e){
                // client closed GUI, should logout before disconnecting
                logout();
            }
        });
    }

    private void logout() {
        term = true;

        JSONObject msgObj = new JSONObject();

        msgObj.put("command", "LOGOUT");

        out.println(msgObj.toJSONString());

        log.info("User logged out, closing connection.");
        closeCon();
        System.exit(0);
    }

    private void openCon(String hostname, int port) {
        try {
            clientSocket = new Socket(hostname, port);

            log.info("Connected to: " + clientSocket);

            out = new PrintWriter(clientSocket.getOutputStream(), true);
            in = new BufferedReader(new InputStreamReader(clientSocket.getInputStream()));

        } catch (IOException e) {
            log.error("Cannot create new client thread: " + e);
            System.exit(-1);
        }

        // Register if username provided
        if (username != null && !username.equals("anonymous")) {
            if (secret == null) {
                // generate new secret
                secret = Settings.nextSecret();
            }

            JSONObject registerMsg = new JSONObject();
            registerMsg.put("command", "REGISTER");
            registerMsg.put("username", username);
            registerMsg.put("secret", secret);

            log.info("Sending register message, username: " + username + ", secret: " + secret);

            out.println(registerMsg.toJSONString());
        } else {
            // login as anonymous user
            JSONObject loginMsg = new JSONObject();
            loginMsg.put("command", "LOGIN");
            loginMsg.put("username", "anonymous");

            log.info("Logging in as anonymous user");

            out.println(loginMsg.toJSONString());

            needRegister = false;
        }

        term = false;
    }

    private void closeCon() {
        term = true;

        try {
            if (out != null) out.close();
            if (in != null) in.close();
            if (clientSocket != null) clientSocket.close();
        } catch (IOException e) {
            // already closed?
            log.error("Received exception closing the connection: "+e);
        }
    }

    @SuppressWarnings("unchecked")
    public void sendActivityObject(JSONObject activityObj) {
        log.info("Sending: " + activityObj.toJSONString());

        // If sending LOGOUT, kill connection
        if (activityObj.get("command") != null && activityObj.get("command").toString().equals("LOGOUT")) {
            logout();
            return;
        }

        out.println(activityObj.toJSONString());
    }


    public void disconnect() {
        log.info("User clicked on disconnect button, killing client now.");
        logout();
    }


    private boolean process(String msg) {
        JSONObject jsonObject;
        JSONParser parser = new JSONParser();

        try {
            jsonObject = (JSONObject) parser.parse(msg);
        } catch (ParseException e) {
            log.error("Cannot parse JSON object: "+ e);
            return true;
        }

        if (jsonObject != null) {

            // waiting for REGISTER_SUCCESS message first
            if (needRegister) {
                if (jsonObject.get("command") == null) {
                    log.error("Invalid message from server");
                    return true;
                }

                if (jsonObject.get("command").equals("REGISTER_SUCCESS")) {
                    // login user with username-secret pair
                    JSONObject loginMsg = new JSONObject();
                    loginMsg.put("command", "LOGIN");
                    loginMsg.put("username", username);
                    loginMsg.put("secret", secret);

                    log.info("Sending login message, username: " + username + ", secret: " + secret);

                    out.println(loginMsg.toJSONString());

                    needRegister = false;
                    return false;
                } else if (jsonObject.get("command").equals("REGISTER_FAILED")) {
                    log.error("Client register failed on server");
                } else {
                    log.error("Invalid message from server, need to register client first");
                }
                return true;
            }

            // waiting for LOGIN_SUCCESS message first
            if (needLogin) {
                if (jsonObject.get("command") == null) {
                    log.error("Invalid message from server");
                    return true;
                }

                if (jsonObject.get("command").equals("LOGIN_SUCCESS")) {
                    needLogin= false;
                    return false;
                } else if (jsonObject.get("command").equals("LOGIN_FAILED")) {
                    log.error("Client login failed on server");
                } else {
                    log.error("Invalid message from server, need to login client first");
                }
                return true;
            }


            textFrame.setOutputText(jsonObject);

            if (jsonObject.get("command") == null) {
                log.error("Invalid message from server");
                return true;
            }

            String command = jsonObject.get("command").toString();

            switch (command) {
                case "REDIRECT":
                    // Close the connection and make a new connection
                    if (jsonObject.get("hostname") == null) {
                        log.error("No hostname specified in message.");
                        return true;
                    }
                    if (jsonObject.get("port") == null) {
                        log.error("No port number specified in message.");
                        return true;
                    }

                    closeCon();
                    openCon(jsonObject.get("hostname").toString(), new Integer(jsonObject.get("port").toString()));

                    break;

                // If received INVALID_MESSAGE, AUTHENTICATION_FAIL, LOGIN_FAILED, REGISTER_FAILED: kill connection.
                case "INVALID_MESSAGE":
                case "AUTHENTICATION_FAIL":
                case "LOGIN_FAILED":
                case "REGISTER_FAILED":
                    term = true;
                    log.info(command + " message received, closing connection.");
                    closeCon();
                    System.exit(0);
            }
        }
        return false;
    }

    /*
     * Reads input from server and prints to console
     */
    public void run() {
        String data;
        while(!term && in != null) {
            if (!guiOpened && !needRegister && !needLogin) {
                openGUI();
                guiOpened = true;
            }
            try {
                data = in.readLine();
                if (data != null) {
                    log.info("Received: "+ data);
                    term = process(data);
                }
            } catch (Exception e){
                log.info("Some error: " + e);
            }
        }

        // Connection should close
        closeCon();
    }
}
