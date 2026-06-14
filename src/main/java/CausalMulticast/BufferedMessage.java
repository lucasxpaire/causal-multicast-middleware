package CausalMulticast;

import java.util.Map;

public class BufferedMessage {

    private String content;
    private String senderId;
    private Map<String, Integer> vectorClock;
    private boolean delivered;

    public BufferedMessage(String content, String senderId, Map<String, Integer> vectorClock) {
        this.content = content;
        this.senderId = senderId;
        this.vectorClock = vectorClock;
        this.delivered = false;
    }

    public String getContent() {
        return content;
    }

    public String getSenderId() {
        return senderId;
    }

    public Map<String, Integer> getVectorClock() {
        return vectorClock;
    }

    public boolean isDelivered() {
        return delivered;
    }

    public void setDelivered(boolean delivered) {
        this.delivered = delivered;
    }
}
