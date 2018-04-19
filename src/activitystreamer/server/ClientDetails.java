package activitystreamer.server;

public class ClientDetails {
    public static String username;
    public static String secret;

    public ClientDetails(String username, String secret) {
        this.username = username;
        this.secret = secret;
    }

    public ClientDetails() {
        this.username = "anonymous";
    }
}
