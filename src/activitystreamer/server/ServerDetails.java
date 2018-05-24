package activitystreamer.server;

public class ServerDetails {
    public String hostname;
    public int port;
    public String serverId;
    public int load;
    public Connection connection;

    public ServerDetails(String serverId, String hostname, int port, int load) {
        this.serverId = serverId;
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
