package CausalMulticast;

import java.net.*;
import java.util.*;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * Serviço de Descoberta dinâmica de membros do grupo baseado em IP Multicast.
 * Executa de forma contínua em threads separadas para anunciar a presença do nó local
 * (através de batimentos cardíacos/heartbeats periódicos) e para escutar anúncios de outros nós,
 * permitindo a atualização em tempo real do grupo de computação cooperativa.
 *  @author -
 * @version 1.0
 */

public class DiscoveryService implements Runnable {
    private static final String MULTICAST_GROUP = "224.0.0.1";
    private static final int MULTICAST_PORT = 4446;
    private static final long HEARTBEAT_INTERVAL = 5000; // 5 segundos

    private final String localId;
    private final int localPort;
    private final CausalMulticast causalMulticast;
    private volatile boolean running = true;
    private final List<String> discoveredPeers = new CopyOnWriteArrayList<>();

    private NetworkInterface networkInterface;

    /**
     * Construtor para o serviço de descoberta dinâmica de peers.
     *  @param localId Identificador único do nó local (normalmente no formato "IP:Porta").
     * @param localPort Porta de rede local alocada para o middleware.
     * @param causalMulticast Referência do motor central de ordenação causal.
     */
    public DiscoveryService(String localId, int localPort, CausalMulticast causalMulticast) {
        this.localId = localId;
        this.localPort = localPort;
        this.causalMulticast = causalMulticast;

        try {
            String localIp = localId.split(":")[0];
            InetAddress localAddr = InetAddress.getByName(localIp);
            this.networkInterface = NetworkInterface.getByInetAddress(localAddr);

            if (this.networkInterface == null) {
                this.networkInterface = NetworkInterface.getByName("lo");
            }
        } catch (Exception e) {
            System.err.println("[DISCOVERY] Não foi possível mapear a placa de rede: " + e.getMessage());
        }
    }

    @Override
    public void run() {
        Thread senderThread = new Thread(this::runHeartbeatSender);
        senderThread.setDaemon(true);
        senderThread.start();

        Thread receiverThread = new Thread(this::runHeartbeatReceiver);
        receiverThread.setDaemon(true);
        receiverThread.start();
    }

    private void runHeartbeatSender() {
        try {
            String localIp = this.localId.split(":")[0];
            InetAddress localAddr = InetAddress.getByName(localIp);

            DatagramSocket socket = new DatagramSocket(new InetSocketAddress(localAddr, 0));
            InetAddress group = InetAddress.getByName(MULTICAST_GROUP);

            System.out.println("[DISCOVERY] Iniciando envio de heartbeats...");

            while (running) {
                String heartbeat = "PEER:" + this.localId;
                byte[] data = heartbeat.getBytes();

                DatagramPacket packet = new DatagramPacket(
                        data, data.length, group, MULTICAST_PORT
                );

                try {
                    socket.send(packet);
                    //System.out.println("[DISCOVERY] Heartbeat enviado: " + this.localId);
                } catch (Exception e) {
                    //System.err.println("[DISCOVERY] Aviso ao enviar heartbeat: " + e.getMessage());
                }

                Thread.sleep(HEARTBEAT_INTERVAL);
            }

            socket.close();
        } catch (Exception e) {
            System.err.println("[DISCOVERY ERRO] Erro no sender: " + e.getMessage());
        }
    }

    private void runHeartbeatReceiver() {
        try {
            // Cria o socket Multicast atrelado à porta do grupo
            MulticastSocket socket = new MulticastSocket(MULTICAST_PORT);
            socket.setReuseAddress(true);
            socket.setSoTimeout(2000);

            // Criamos a estrutura de endereço moderna (IP do grupo + Porta)
            SocketAddress groupAddress = new InetSocketAddress(InetAddress.getByName(MULTICAST_GROUP), MULTICAST_PORT);

            // CORREÇÃO MODERNA: Entra no grupo informando explicitamente qual placa de rede usar
            if (this.networkInterface != null) {
                socket.joinGroup(groupAddress, this.networkInterface);
            } else {
                // Caso extremo onde não foi mapeada, tenta o padrão do sistema (pode falhar se houver VPN)
                socket.joinGroup(new InetSocketAddress(InetAddress.getByName(MULTICAST_GROUP), 0), null);
            }

            System.out.println("[DISCOVERY] Aguardando heartbeats de outros peers...");

            byte[] buffer = new byte[256];

            while (running) {
                try {
                    DatagramPacket packet = new DatagramPacket(buffer, buffer.length);
                    socket.receive(packet);

                    String message = new String(packet.getData(), 0, packet.getLength()).trim();

                    if (message.startsWith("PEER:")) {
                        String peerId = message.substring(5);

                        // Como removemos o loopbackMode depreciado, filtramos nós mesmos de forma lógica aqui:
                        if (!peerId.equals(this.localId) && !discoveredPeers.contains(peerId)) {
                            discoveredPeers.add(peerId);
                            System.out.println("[DISCOVERY] Novo peer descoberto: " + peerId);

                            List<String> allPeers = new ArrayList<>(discoveredPeers);
                            allPeers.add(this.localId);
                            this.causalMulticast.updateGroupMembers(allPeers);
                        }
                    }
                } catch (SocketTimeoutException e) {
                    continue; // Timeout controlado para verificar se 'running' ainda é true
                }
            }

            // CORREÇÃO MODERNA: Sai do grupo explicitamente usando a mesma assinatura
            try {
                socket.leaveGroup(groupAddress, this.networkInterface);
            } catch (Exception e) {
                // Ignora falhas na limpeza de fechamento
            }
            socket.close();
        } catch (Exception e) {
            System.err.println("[DISCOVERY ERRO] Erro no receiver: " + e.getMessage());
        }
    }

    /**
     * Solicita a interrupção segura e limpa de todas as tarefas de background do serviço.
     */
    public void stop() {
        running = false;
    }

    /**
     * Fornece uma cópia isolada e thread-safe contendo a lista de todos os peers
     * remotos descobertos dinamicamente na rede.
     *  @return Uma {@link List} contendo as strings identificadoras dos peers ativos.
     */
    public List<String> getDiscoveredPeers() {
        return new ArrayList<>(discoveredPeers);
    }
}