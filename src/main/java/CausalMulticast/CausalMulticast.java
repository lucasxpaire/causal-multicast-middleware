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
    private UDPSender udpSender;

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

        try {
            this.udpSender = new UDPSender();
        } catch (Exception e) {
            System.err.println("[MIDDLEWARE ERROR] Falha ao inicializar o UDPSender: " + e.getMessage());
        }

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
        Integer localPeerIndex = this.peerToIndex.get(this.localId);
        if (localPeerIndex == null) {
            return;
        }

        Map<String, Integer> currentVectorClock = new ConcurrentHashMap<>();
        synchronized (this) {
            // Copia o relógio local atual para anexar na mensagem
            for (String peer : this.activePeers) {
                Integer peerIndex = this.peerToIndex.get(peer);
                if (peerIndex != null) {
                    currentVectorClock.put(peer, this.matrixClock[localPeerIndex][peerIndex]);
                }
            }
        }

        BufferedMessage message = new BufferedMessage(msg, this.localId, currentVectorClock);
        sendToGroup(message);

        synchronized (this) {
            localPeerIndex = this.peerToIndex.get(this.localId);
            if (localPeerIndex != null) {
                // Incrementa a própria posição do relógio local pós-envio
                this.matrixClock[localPeerIndex][localPeerIndex] = this.matrixClock[localPeerIndex][localPeerIndex] + 1;
            }
        }
    }

    private void sendToGroup(BufferedMessage message) {
        for (String peer: this.activePeers){
            if (!peer.equals(this.localId)){
                String[] parts = peer.split(":");
                String ip = parts[0];
                int port = Integer.parseInt(parts[1]);

                udpSender.sendMessage(ip, port, message);
            }
        }
    }

    /**
     * Processa uma mensagem vinda da rede, aplicando ordenamento causal e estabilização.
     * 
     * @param message A mensagem recebida da rede.
     */
    public void onMessageReceived(BufferedMessage message) {
        String senderId = message.getSenderId();

        synchronized (this) {
            Integer senderIndex = this.peerToIndex.get(senderId);
            Integer localPeerIndex = this.peerToIndex.get(this.localId);

            if (senderIndex == null || localPeerIndex == null) {
                return;
            }

            // 1. Filtro de Mensagens Duplicadas
            int messageSeqNumber = message.getVectorClock().getOrDefault(senderId, 0);
            int deliveredCount;
            if (senderId.equals(this.localId)) {
                deliveredCount = this.localMessagesDelivered;
            } else {
                deliveredCount = this.matrixClock[localPeerIndex][senderIndex];
            }
            if (messageSeqNumber < deliveredCount) {
                return;
            }

            // 2. Atualiza a linha da matriz do remetente com o relógio que veio na mensagem
            for (String peer : this.activePeers) {
                Integer colIndex = this.peerToIndex.get(peer);
                if (colIndex != null) {
                    int peerClockValue = message.getVectorClock().getOrDefault(peer, 0);
                    this.matrixClock[senderIndex][colIndex] = peerClockValue;
                }
            }

            // 3. Insere a mensagem no buffer de espera
            this.messageBuffer.add(message);

            // 4. Verifica se a mensagem pode ser entregue à aplicação
            boolean newDelivery;
            do {
                newDelivery = false;
                for (BufferedMessage bufferedMsg : this.messageBuffer) {
                    if (!bufferedMsg.isDelivered()) {
                        String msgSender = bufferedMsg.getSenderId();
                        Integer msgSenderIndex = this.peerToIndex.get(msgSender);

                        if (msgSenderIndex != null) {
                            // Verifica se todas as mensagens causais precedentes já foram entregues
                            boolean canDeliver = true;
                            for (String peer : this.activePeers) {
                                Integer peerIndex = this.peerToIndex.get(peer);
                                if (peerIndex != null) {
                                    int messageClockValue = bufferedMsg.getVectorClock().getOrDefault(peer, 0);
                                    int localClockValue = this.matrixClock[localPeerIndex][peerIndex];

                                    if (messageClockValue > localClockValue) {
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
                                    this.matrixClock[localPeerIndex][msgSenderIndex] = this.matrixClock[localPeerIndex][msgSenderIndex] + 1;
                                }

                                this.client.deliver(bufferedMsg.getContent());

                                newDelivery = true;
                                break;
                            }
                        }
                    }
                }
            } while (newDelivery);

            // 5. Estabilização e Descarte do Buffer
            List<BufferedMessage> stableMessages = new ArrayList<>();
            for (BufferedMessage bufferedMsg : this.messageBuffer) {
                if (bufferedMsg.isDelivered()) {
                    String msgSender = bufferedMsg.getSenderId();
                    Integer msgSenderIndex = this.peerToIndex.get(msgSender);

                    if (msgSenderIndex != null) {
                        int msgSeq = bufferedMsg.getVectorClock().getOrDefault(msgSender, 0);

                        // Encontra o menor relógio registrado para o remetente entre todos os nós
                        int minReceived = Integer.MAX_VALUE;
                        for (String peer : this.activePeers) {
                            Integer peerIndex = this.peerToIndex.get(peer);
                            if (peerIndex != null) {
                                int peerClockValue = this.matrixClock[peerIndex][msgSenderIndex];
                                if (peerClockValue < minReceived) {
                                    minReceived = peerClockValue;
                                }
                            }
                        }

                        if (msgSeq <= minReceived) {
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
