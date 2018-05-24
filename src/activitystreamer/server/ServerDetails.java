package activitystreamer.server;

public class ServerDetails {
    public String hostname;
    public int port;
    public String serverId;
    public String prevId; // keeps track of incomingServer for this server
    public String nextId; // keeps track of outgoingServer for this server
    public int load;
    public Connection connection;

    public ServerDetails(String serverId, String prevId, String nextId, String hostname, int port, int load) {
        this.serverId = serverId;
        this.prevId = prevId; // nullable
        this.nextId = nextId; // nullable
        this.hostname = hostname;
        this.port = port;
        this.load = load;
    }

    public ServerDetails(String serverId, Connection con, String hostname, int port) {
        this.serverId = serverId;
        this.connection = con;
        this.hostname = hostname;
        this.port = port;
    }
}
