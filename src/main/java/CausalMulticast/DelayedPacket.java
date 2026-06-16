package CausalMulticast;

/**
 * Representa um pacote cuja transmissão física UDP foi retida no emissor.
 */
public class DelayedPacket {
    private final String peerId;
    private final String ip;
    private final int port;
    private final BufferedMessage message;

    public DelayedPacket(String peerId, String ip, int port, BufferedMessage message) {
        this.peerId = peerId;
        this.ip = ip;
        this.port = port;
        this.message = message;
    }

    public String getPeerId() { return peerId; }
    public String getIp() { return ip; }
    public int getPort() { return port; }
    public BufferedMessage getMessage() { return message; }
}