package CausalMulticast;

import java.util.Map;

/**
 * Representa o envelope estruturado de uma mensagem encapsulada no nível do middleware.
 * Transporta o payload textual puro fornecido pela aplicação cliente juntamente com os
 * metadados de controle necessários para garantir a ordenação causal e a estabilização global
 * (identificação do nó remetente e o estado de seu relógio vetorial).
 */
public class BufferedMessage {

    /** O conteúdo ou carga útil (payload) em texto puro transmitido pela mensagem. */
    private final String content;

    /** O identificador exclusivo do nó que originou e enviou a mensagem, formatado como "IP:Porta". */
    private final String senderId;

    /** O carimbo de data/hora lógico (Relógio Vetorial) capturado no nó remetente no instante exato do envio. */
    private final Map<String, Integer> vectorClock;

    /** Flag de controle indicando se esta mensagem específica já foi validada e entregue à aplicação local. */
    private boolean delivered;

    /**
     * Construtor para inicializar uma mensagem no middleware.
     * Define o estado inicial da mensagem marcando-a preventivamente como não entregue.
     *
     * @param content Conteúdo textual puro da mensagem.
     * @param senderId Identificador único do remetente (IP:Porta).
     * @param vectorClock Relógio vetorial do remetente no momento do envio.
     */
    public BufferedMessage(String content, String senderId, Map<String, Integer> vectorClock) {
        this.content = content;
        this.senderId = senderId;
        this.vectorClock = vectorClock;
        this.delivered = false;
    }

    /**
     * Retorna o conteúdo textual bruto armazenado no payload da mensagem.
     *
     * @return O texto contido na mensagem.
     */
    public String getContent() {
        return content;
    }

    /**
     * Retorna o identificador de rede do nó responsável pela criação e transmissão original da mensagem.
     *
     * @return O ID do remetente no formato "IP:Porta".
     */
    public String getSenderId() {
        return senderId;
    }

    /**
     * Retorna o Relógio Vetorial (mapa de IDs para contadores lógicos) associado a esta mensagem.
     * Representa o estado de causalidade capturado no momento da emissão.
     *
     * @return O mapa representando o carimbo do relógio vetorial.
     */
    public Map<String, Integer> getVectorClock() {
        return vectorClock;
    }

    /**
     * Verifica se a mensagem já passou pelo algoritmo de validação causal e foi
     * liberada com sucesso para exibição/consumo na camada da aplicação cliente.
     *
     * @return {@code true} se a mensagem já tiver sido entregue; {@code false} caso contrário.
     */
    public boolean isDelivered() {
        return delivered;
    }

    /**
     * Altera o estado de entrega da mensagem no middleware após a validação bem-sucedida das regras causais.
     *
     * @param delivered O novo estado de entrega para marcar a mensagem (normalmente {@code true}).
     */
    public void setDelivered(boolean delivered) {
        this.delivered = delivered;
    }
}