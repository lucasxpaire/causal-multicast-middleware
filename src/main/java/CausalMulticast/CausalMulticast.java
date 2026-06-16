package CausalMulticast;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;

public class CausalMulticast {

    private final String localId;
    private final ICausalMulticast client;
    private final List<String> activePeers;
    private Map<String, Integer> peerToIndex;
    private int[][] matrixClock;
    private final List<BufferedMessage> messageBuffer;
    private int localMessagesDelivered = 0;

    /**
     * Inicializa o middleware de multicast causal para o nó local.
     * 
     * @param ip IP unicast local.
     * @param port Porta unicast local.
     * @param client Referência de retorno da aplicação cliente.
     */
    public CausalMulticast(String ip, Integer port, ICausalMulticast client) {
        this.localId = ip + ":" + port;
        this.client = client;
        this.activePeers = new CopyOnWriteArrayList<>();
        this.peerToIndex = new ConcurrentHashMap<>();
        this.messageBuffer = Collections.synchronizedList(new ArrayList<BufferedMessage>());
        this.updateGroupMembers(List.of(this.localId));
    }

    /**
     * Atualiza os membros do grupo e redimensiona a matriz de relógios preservando o histórico.
     * 
     * @param newPeers Nova lista de membros ativos.
     */
    public synchronized void updateGroupMembers(List<String> newPeers) {
        List<String> sortedPeers = new ArrayList<>(newPeers);
        if (!sortedPeers.contains(localId)) {
            sortedPeers.add(localId);
        }
        Collections.sort(sortedPeers);

        Map<String, Integer> oldPeerToIndex = this.peerToIndex;
        int[][] oldMatrixClock = this.matrixClock;

        this.activePeers.clear();
        this.activePeers.addAll(sortedPeers);

        int newSize = sortedPeers.size();
        Map<String, Integer> newPeerToIndex = new ConcurrentHashMap<>();
        for (int i = 0; i < newSize; i++) {
            newPeerToIndex.put(sortedPeers.get(i), i);
        }
        this.peerToIndex = newPeerToIndex;

        int[][] newMatrixClock = new int[newSize][newSize];

        if (oldMatrixClock != null && oldPeerToIndex != null) {
            for (String rowPeer : oldPeerToIndex.keySet()) {
                if (newPeerToIndex.containsKey(rowPeer)) {
                    int oldRowIdx = oldPeerToIndex.get(rowPeer);
                    int newRowIdx = newPeerToIndex.get(rowPeer);
                    for (String colPeer : oldPeerToIndex.keySet()) {
                        if (newPeerToIndex.containsKey(colPeer)) {
                            int oldColIdx = oldPeerToIndex.get(colPeer);
                            int newColIdx = newPeerToIndex.get(colPeer);
                            newMatrixClock[newRowIdx][newColIdx] = oldMatrixClock[oldRowIdx][oldColIdx];
                        }
                    }
                }
            }
        }
        this.matrixClock = newMatrixClock;
    }

    /**
     * Envia uma mensagem multicast para o grupo garantindo a ordenação causal.
     * 
     * @param msg Texto da mensagem a ser enviada.
     * @param cliente Referência de callback do cliente.
     */
    public void mcsend(String msg, ICausalMulticast cliente) {
        Integer localPeerIdx = this.peerToIndex.get(this.localId);
        if (localPeerIdx == null) {
            return;
        }

        Map<String, Integer> currentVectorClock = new ConcurrentHashMap<>();
        synchronized (this) {
            for (String peer : this.activePeers) {
                Integer peerIdx = this.peerToIndex.get(peer);
                if (peerIdx != null) {
                    currentVectorClock.put(peer, this.matrixClock[localPeerIdx][peerIdx]);
                }
            }
        }

        BufferedMessage message = new BufferedMessage(msg, this.localId, currentVectorClock);
        sendToGroup(message);

        synchronized (this) {
            localPeerIdx = this.peerToIndex.get(this.localId);
            if (localPeerIdx != null) {
                this.matrixClock[localPeerIdx][localPeerIdx] = this.matrixClock[localPeerIdx][localPeerIdx] + 1;
            }
        }
    }

    private void sendToGroup(BufferedMessage message) {
        // TODO: implementar a transmissão UDP
    }

    /**
     * Processa uma mensagem vinda da rede, aplicando ordenamento causal e estabilização.
     * 
     * @param message A mensagem recebida da rede.
     */
    public void onMessageReceived(BufferedMessage message) {
        String senderId = message.getSenderId();

        synchronized (this) {
            Integer senderIdx = this.peerToIndex.get(senderId);
            Integer localPeerIdx = this.peerToIndex.get(this.localId);

            if (senderIdx == null || localPeerIdx == null) {
                return;
            }

            int msgSeq = message.getVectorClock().getOrDefault(senderId, 0);
            int deliveredCount;
            if (senderId.equals(this.localId)) {
                deliveredCount = this.localMessagesDelivered;
            } else {
                deliveredCount = this.matrixClock[localPeerIdx][senderIdx];
            }
            if (msgSeq < deliveredCount) {
                return;
            }

            for (String peer : this.activePeers) {
                Integer colIdx = this.peerToIndex.get(peer);
                if (colIdx != null) {
                    this.matrixClock[senderIdx][colIdx] = message.getVectorClock().getOrDefault(peer, 0);
                }
            }

            this.messageBuffer.add(message);

            boolean newDelivery;
            do {
                newDelivery = false;
                for (BufferedMessage bufferedMsg : this.messageBuffer) {
                    if (!bufferedMsg.isDelivered()) {
                        String msgSender = bufferedMsg.getSenderId();
                        Integer msgSenderIdx = this.peerToIndex.get(msgSender);

                        if (msgSenderIdx != null) {
                            boolean canDeliver = true;
                            for (String peer : this.activePeers) {
                                Integer peerIdx = this.peerToIndex.get(peer);
                                if (peerIdx != null) {
                                    int msgClockVal = bufferedMsg.getVectorClock().getOrDefault(peer, 0);
                                    int localClockVal = this.matrixClock[localPeerIdx][peerIdx];

                                    if (msgClockVal > localClockVal) {
                                        canDeliver = false;
                                        break;
                                    }
                                }
                            }

                            if (canDeliver) {
                                bufferedMsg.setDelivered(true);

                                if (msgSender.equals(this.localId)) {
                                    this.localMessagesDelivered++;
                                } else {
                                    this.matrixClock[localPeerIdx][msgSenderIdx] = this.matrixClock[localPeerIdx][msgSenderIdx] + 1;
                                }

                                this.client.deliver(bufferedMsg.getContent());

                                newDelivery = true;
                                break;
                            }
                        }
                    }
                }
            } while (newDelivery);

            List<BufferedMessage> stableMessages = new ArrayList<>();
            for (BufferedMessage bufferedMsg : this.messageBuffer) {
                if (bufferedMsg.isDelivered()) {
                    String msgSender = bufferedMsg.getSenderId();
                    Integer msgSenderIdx = this.peerToIndex.get(msgSender);

                    if (msgSenderIdx != null) {
                        int msgSeqNumber = bufferedMsg.getVectorClock().getOrDefault(msgSender, 0);

                        // Encontra o menor relógio registrado para este remetente entre  todos do grupo
                        int minReceived = Integer.MAX_VALUE;
                        for (String peer : this.activePeers) {
                            Integer peerIdx = this.peerToIndex.get(peer);
                            if (peerIdx != null) {
                                int val = this.matrixClock[peerIdx][msgSenderIdx];
                                if (val < minReceived) {
                                    minReceived = val;
                                }
                            }
                        }

                        // Se todos os nós já receberam pelo menos essa mensagem, ela é estável
                        if (msgSeqNumber <= minReceived) {
                            stableMessages.add(bufferedMsg);
                        }
                    }
                }
            }
            this.messageBuffer.removeAll(stableMessages);
        }
    }

    /**
     * Retorna a matriz de relógios lógicos formatada em texto.
     * 
     * @return Tabela de relógios lógicos.
     */
    public synchronized String getMatrixClockState() {
        StringBuilder sb = new StringBuilder();
        sb.append("\n=================== MATRIZ DE RELÓGIOS ===================\n");
        for (String rowPeer : this.activePeers) {
            Integer rowIdx = this.peerToIndex.get(rowPeer);
            if (rowIdx != null) {
                String suffix = "";
                if (rowPeer.equals(this.localId)) {
                    suffix = " (Você)";
                }
                sb.append(rowPeer).append(suffix).append(" -> [");

                for (int col = 0; col < this.activePeers.size(); col++) {
                    sb.append(this.matrixClock[rowIdx][col]);
                    if (col < this.activePeers.size() - 1) {
                        sb.append(", ");
                    }
                }
                sb.append("]\n");
            }
        }
        sb.append("==========================================================");
        return sb.toString();
    }

    /**
     * Retorna a fila de mensagens do buffer formatada em texto.
     * 
     * @return Fila de mensagens em espera.
     */
    public synchronized String getBufferState() {
        StringBuilder sb = new StringBuilder();
        sb.append("\n================== BUFFER DE MENSAGENS ==================\n");
        sb.append("Total de mensagens na fila: ").append(this.messageBuffer.size()).append("\n");

        List<BufferedMessage> bufferCopy = new ArrayList<>(this.messageBuffer);
        for (BufferedMessage msg : bufferCopy) {
            sb.append("- De: ").append(msg.getSenderId())
                    .append(" | Conteúdo: \"").append(msg.getContent()).append("\"")
                    .append(" | Relógio da Mensagem: ").append(msg.getVectorClock())
                    .append(" | Entregue à Tela: ").append(msg.isDelivered())
                    .append("\n");
        }
        sb.append("=========================================================");
        return sb.toString();
    }
}

