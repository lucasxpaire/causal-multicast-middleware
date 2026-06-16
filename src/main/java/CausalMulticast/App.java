package CausalMulticast;

import java.util.*;
import java.util.concurrent.CopyOnWriteArrayList;

public class App implements ICausalMulticast {
    private CausalMulticast causalMulticast;
    private DelayQueue delayQueue;
    private DiscoveryService discoveryService;
    private UDPReceiver udpReceiver;
    private String localId;
    private int localPort;
    private final List<String> deliveredMessages = new CopyOnWriteArrayList<>();

    public static void main(String[] args) {
        Scanner scanner = new Scanner(System.in);

        System.out.println("=== CAUSAL MULTICAST MIDDLEWARE ===");
        System.out.print("Digite o IP local (ex: 192.168.1.100): ");
        String ip = scanner.nextLine().trim();

        System.out.print("Digite a porta local (ex: 5000): ");
        int port = Integer.parseInt(scanner.nextLine().trim());

        App app = new App(ip, port);
        app.start();
        app.runInteractiveMenu(scanner);
    }

    public App(String ip, int port) {
        this.localId = ip + ":" + port;
        this.localPort = port;
        this.causalMulticast = new CausalMulticast(ip, port, this);
        this.delayQueue = new DelayQueue(causalMulticast);
    }

    public void start() {
        try {
            // Iniciar UDP Receiver
            this.udpReceiver = new UDPReceiver(localPort, causalMulticast, delayQueue);
            Thread udpThread = new Thread(udpReceiver);
            udpThread.setDaemon(false);
            udpThread.start();

            // Iniciar Discovery Service
            this.discoveryService = new DiscoveryService(localId, localPort, causalMulticast);
            Thread discoveryThread = new Thread(discoveryService);
            discoveryThread.setDaemon(false);
            discoveryThread.start();

            // Carregar peers do arquivo peers.conf (fallback se multicast falhar)
            loadPeersFromConfig();

            System.out.println("\n[APP] Sistema iniciado com sucesso!");
            System.out.println("[APP] ID Local: " + localId);

        } catch (Exception e) {
            System.err.println("[APP ERROR] Erro ao iniciar: " + e.getMessage());
            e.printStackTrace();
        }
    }

    private void runInteractiveMenu(Scanner scanner) {
        boolean running = true;

        while (running) {
            System.out.println("\n===== MENU =====");
            System.out.println("1. Enviar mensagem");
            System.out.println("2. Ver matriz de relógios");
            System.out.println("3. Ver buffer de mensagens");
            System.out.println("4. Configurar atraso de peer");
            System.out.println("5. Ver mensagens entregues");
            System.out.println("6. Ver peers descobertos");
            System.out.println("7. Sair");
            System.out.print("Escolha uma opção: ");

            String choice = scanner.nextLine().trim();

            switch (choice) {
                case "1":
                    sendMessage(scanner);
                    break;
                case "2":
                    viewMatrixClock();
                    break;
                case "3":
                    viewMessageBuffer();
                    break;
                case "4":
                    configurePeerDelay(scanner);
                    break;
                case "5":
                    viewDeliveredMessages();
                    break;
                case "6":
                    viewDiscoveredPeers();
                    break;
                case "7":
                    running = false;
                    shutdown();
                    break;
                default:
                    System.out.println("Opção inválida!");
            }
        }

        scanner.close();
    }

    private void sendMessage(Scanner scanner) {
        System.out.print("Digite a mensagem: ");
        String message = scanner.nextLine().trim();

        if (!message.isEmpty()) {
            causalMulticast.mcsend(message, this);
            System.out.println("[APP] Mensagem enviada!");
        }
    }

    private void viewMatrixClock() {
        System.out.println(causalMulticast.getMatrixClockState());
    }

    private void viewMessageBuffer() {
        System.out.println(causalMulticast.getBufferState());
    }

    private void configurePeerDelay(Scanner scanner) {
        System.out.println("\nPeers conhecidos:");
        List<String> peers = new ArrayList<>(discoveryService.getDiscoveredPeers());
        for (int i = 0; i < peers.size(); i++) {
            System.out.println((i + 1) + ". " + peers.get(i));
        }

        System.out.print("Escolha o número do peer: ");
        int peerIndex = Integer.parseInt(scanner.nextLine().trim()) - 1;

        if (peerIndex >= 0 && peerIndex < peers.size()) {
            String peerId = peers.get(peerIndex);
            System.out.print("Digite o atraso em milissegundos: ");
            long delay = Long.parseLong(scanner.nextLine().trim());

            delayQueue.setPeerDelay(peerId, delay);
            System.out.println("[APP] Atraso configurado: " + delay + "ms para " + peerId);
        } else {
            System.out.println("Peer inválido!");
        }
    }

    private void viewDeliveredMessages() {
        System.out.println("\n===== MENSAGENS ENTREGUES =====");
        if (deliveredMessages.isEmpty()) {
            System.out.println("Nenhuma mensagem entregue ainda.");
        } else {
            for (int i = 0; i < deliveredMessages.size(); i++) {
                System.out.println((i + 1) + ". " + deliveredMessages.get(i));
            }
        }
    }

    private void viewDiscoveredPeers() {
        System.out.println("\n===== PEERS DESCOBERTOS =====");
        List<String> peers = discoveryService.getDiscoveredPeers();
        if (peers.isEmpty()) {
            System.out.println("Nenhum peer descoberto ainda.");
        } else {
            for (String peer : peers) {
                System.out.println("- " + peer);
            }
        }
    }

    private void shutdown() {
        System.out.println("\n[APP] Encerrando sistema...");
        if (udpReceiver != null) {
            udpReceiver.stop();
        }
        if (discoveryService != null) {
            discoveryService.stop();
        }
        if (delayQueue != null) {
            delayQueue.shutdown();
        }
        System.out.println("[APP] Sistema encerrado.");
        System.exit(0);
    }

    @Override
    public void deliver(String msg) {
        String timestamp = new java.text.SimpleDateFormat("HH:mm:ss.SSS").format(new Date());
        String formattedMessage = "[" + timestamp + "] " + msg;
        deliveredMessages.add(formattedMessage);
        System.out.println("\n>>> MENSAGEM ENTREGUE: " + formattedMessage);
    }
}