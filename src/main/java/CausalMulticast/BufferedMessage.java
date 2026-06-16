package CausalMulticast;

import java.util.Map;

/**
 * Representa o envelope de uma mensagem, contendo o texto e os metadados causais.
 */
public class BufferedMessage {

    private final String content;
    private final String senderId;
    private final Map<String, Integer> vectorClock;
    private boolean delivered;
    private int sequenceNumber;

    /**
     * Construtor para inicializar uma mensagem no middleware.
     *
     * @param content Conteúdo textual da mensagem.
     * @param senderId Identificador único do remetente (IP:Porta).
     * @param vectorClock Relógio vetorial do remetente no momento do envio.
     */
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
