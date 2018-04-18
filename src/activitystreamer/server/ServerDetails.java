package activitystreamer.server;

public class ServerDetails {
    public static String hostname;
    public static int port;
    public static String serverId;
    public int load;

    public ServerDetails(String hostname, int port, String serverId, int load) {
        this.hostname = hostname;
        this.port = port;
        this.serverId = serverId;
        this.load = load;
    }
}
