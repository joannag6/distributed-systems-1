package activitystreamer.server;

public class ServerDetails {
    public static String hostname;
    public static int port;
    public static String serverId;
    public int load;

    public ServerDetails(String serverId, String hostname, int port, int load) {
        this.serverId = serverId;
        this.hostname = hostname;
        this.port = port;
        this.load = load;
    }
}
