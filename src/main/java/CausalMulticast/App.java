package CausalMulticast;

import java.util.*;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * Interface de Console Interativa e Ponto de Entrada (Application Node) do sistema.
 * Implementa a interface {@link ICausalMulticast} para atuar como o cliente que
 * consome as mensagens entregues de forma ordenada pelo middleware. Gerencia o ciclo
 * de vida dos servi�os de rede subjacentes (Descoberta, Envio e Recep��o).
 *  @author Seu Nome
 * @version 1.0
 */
public class App implements ICausalMulticast {

    /** Inst�ncia principal do middleware de ordena��o causal associada a este n�. */
    private CausalMulticast causalMulticast;

    /** Estrutura respons�vel pela simula��o de atraso artificial na entrega de pacotes de rede. */
    private DelayQueue delayQueue;

    /** Servi�o peri�dico baseado em Multicast UDP para an�ncio e descoberta autom�tica de peers na rede local. */
    private DiscoveryService discoveryService;

    /** Thread encarregada de escutar a porta UDP local de forma ininterrupta para receber mensagens f�sicas. */
    private UDPReceiver udpReceiver;

    /** Identificador exclusivo formatado para o n� local no padr�o "IP:Porta". */
    private String localId;

    /** Porta de comunica��o Unicast UDP configurada para este processo. */
    private int localPort;

    /** Lista segura para threads que mant�m o hist�rico de strings j� entregues em ordem l�gica e exibidas na console. */
    private final List<String> deliveredMessages = new CopyOnWriteArrayList<>();

    /**
     * Ponto de entrada padr�o da aplica��o (Main Method).
     * Solicita as configura��es de rede iniciais via console, instancia o n� do sistema,
     * inicializa os daemons em background e aciona o la�o do menu interativo.
     *  @param args Argumentos de linha de comando (n�o utilizados).
     */
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

    /**
     * Construtor da Aplica��o Cliente.
     * Mapeia os dados do n� local e realiza a instancia��o acoplada do middleware
     * injetando a si mesmo como o receptor final das mensagens ordenadas causais.
     *  @param ip Endere�o IP local que ser� associado ao n�.
     * @param port Porta de comunica��o local que ser� aberta para o n�.
     */
    public App(String ip, int port) {
        this.localId = ip + ":" + port;
        this.localPort = port;
        this.causalMulticast = new CausalMulticast(ip, port, this);
        this.delayQueue = new DelayQueue(causalMulticast);
    }

    /**
     * Inicializa os servi�os de rede executados concorrentemente.
     * Instancia e dispara as threads ass�ncronas do {@link UDPReceiver} e do
     * {@link DiscoveryService} configurando-as para manter a execu��o ativa do sistema.
     */
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

            System.out.println("\n[APP] Sistema iniciado com sucesso!");
            System.out.println("[APP] ID Local: " + localId);

        } catch (Exception e) {
            System.err.println("[APP ERROR] Erro ao iniciar: " + e.getMessage());
            e.printStackTrace();
        }
    }

    /**
     * Loop principal que exibe e gerencia o menu de linha de comando interativo do n�.
     *  @param scanner O leitor de entrada padr�o do sistema associado � console.
     */
    private void runInteractiveMenu(Scanner scanner) {
        boolean running = true;

        while (running) {
            System.out.println("\n===== MENU =====");
            System.out.println("1. Enviar mensagem");
            System.out.println("2. Ver matriz de rel�gios");
            System.out.println("3. Ver buffer de mensagens");
            System.out.println("4. Configurar atraso de peer");
            System.out.println("5. Ver mensagens entregues");
            System.out.println("6. Ver peers descobertos");
            System.out.println("7. Enviar mensagens retidas/atrasadas (Emissor)");
            System.out.println("8. Sair");
            System.out.print("Escolha uma op��o: ");

            String choice = scanner.nextLine().trim();

            switch (choice) {
                case "1": sendMessage(scanner); break;
                case "2": viewMatrixClock(); break;
                case "3": viewMessageBuffer(); break;
                case "4": configurePeerDelay(scanner); break;
                case "5": viewDeliveredMessages(); break;
                case "6": viewDiscoveredPeers(); break;
                case "7": sendPendingOutgoingMessages(); break;
                case "8": running = false; shutdown(); break;
                default: System.out.println("Opcão inválida!");
            }
        }

        scanner.close();
    }

    /**
     * Captura uma mensagem em texto digitada pelo operador na console e repassa
     * para o m�todo {@code mcsend} do middleware para propaga��o confi�vel e causal.
     *  @param scanner O leitor de entrada padr�o associado � console.
     */
    /**
     * Intercepta o envio de mensagens realizando perguntas via teclado para cada destino.
     * Cumpre estritamente as exigências do Requisito 7.
     */
    private void sendMessage(Scanner scanner) {
        System.out.print("Digite a mensagem: ");
        String text = scanner.nextLine().trim();

        if (text.isEmpty()) return;

        // 1. Prepara e carimba a mensagem no middleware
        BufferedMessage message = causalMulticast.mcsend(text);
        if (message == null) return;

        // 2. Busca os peers ativos no grupo no momento do envio
        List<String> destinations = causalMulticast.getActivePeers();

        System.out.print("Deseja enviar para TODOS imediatamente? (S/N): ");
        String sendAll = scanner.nextLine().trim().toUpperCase();

        if (sendAll.equals("S")) {
            for (String peer : destinations) {
                if (!peer.equals(causalMulticast.getLocalId())) {
                    causalMulticast.sendUnicastDirect(peer, message);
                }
            }
            System.out.println("[APP] Mensagem enviada para todos!");
        } else {
            // Caso Não, pergunta individualmente peer por peer
            for (String peer : destinations) {
                if (peer.equals(causalMulticast.getLocalId())) continue;

                System.out.print("Deseja enviar imediatamente para o peer " + peer + "? (S/N): ");
                String choice = scanner.nextLine().trim().toUpperCase();

                if (choice.equals("S")) {
                    causalMulticast.sendUnicastDirect(peer, message);
                } else {
                    // Retencão física no lado do emissor
                    String[] parts = peer.split(":");
                    DelayedPacket delayedPacket = new DelayedPacket(peer, parts[0], Integer.parseInt(parts[1]), message);
                    causalMulticast.getOutgoingDelayedQueue().add(delayedPacket);
                    System.out.println("[RETENcÃO] Mensagem para " + peer + " retida fisicamente no emissor.");
                }
            }
        }
    }

    /**
     * Imprime na sa�da padr�o a tabela textual formatada correspondente ao
     * estado atual de conhecimento global mantido pela Matriz de Rel�gios do middleware.
     */
    private void viewMatrixClock() {
        System.out.println(causalMulticast.getMatrixClockState());
    }

    /**
     * Imprime na sa�da padr�o a lista de mensagens recebidas que ainda encontram-se
     * represadas no buffer do middleware aguardando sua valida��o causal ou estabiliza��o.
     */
    private void viewMessageBuffer() {
        System.out.println(causalMulticast.getBufferState());
    }

    /**
     * Permite ao operador selecionar um dos peers descobertos automaticamente e configurar
     * um tempo de reten��o artificial (atraso de rede em ms) para os pacotes oriundos dele.
     *  @param scanner O leitor de entrada padr�o associado � console.
     */
    private void configurePeerDelay(Scanner scanner) {
        System.out.println("\nPeers conhecidos:");
        List<String> peers = new ArrayList<>(discoveryService.getDiscoveredPeers());
        for (int i = 0; i < peers.size(); i++) {
            System.out.println((i + 1) + ". " + peers.get(i));
        }

        System.out.print("Escolha o n�mero do peer: ");
        int peerIndex = Integer.parseInt(scanner.nextLine().trim()) - 1;

        if (peerIndex >= 0 && peerIndex < peers.size()) {
            String peerId = peers.get(peerIndex);
            System.out.print("Digite o atraso em milissegundos: ");
            long delay = Long.parseLong(scanner.nextLine().trim());

            delayQueue.setPeerDelay(peerId, delay);
            System.out.println("[APP] Atraso configurado: " + delay + "ms para " + peerId);
        } else {
            System.out.println("Peer inv�lido!");
        }
    }

    /**
     * Imprime na console todo o hist�rico de mensagens de texto limpas que o algoritmo causal
     * j� validou, ordenou e liberou com sucesso para a aplica��o.
     */
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

    /**
     * Exibe na console os endere�os de rede dos peers externos e ativos descobertos na rede local
     * pelo {@link DiscoveryService}.
     */
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

    /**
     * Realiza o desligamento limpo e ordenado do n� local.
     * Encerra os loops dos sockets receptores e de descoberta, al�m de desligar os
     * pools de agendamento de tarefas da fila de atrasos para evitar vazamentos de mem�ria.
     */
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

    /**
     * Implementa��o do m�todo de callback do contrato {@link ICausalMulticast}.
     * Este m�todo � invocado assincronamente pela camada do middleware assim que uma
     * mensagem atende a todos os crit�rios de preced�ncia causal, registrando o timestamp de libera��o.
     *  @param msg Conte�do textual puro da mensagem que foi validada e entregue.
     */
    @Override
    public void deliver(String msg) {
        String timestamp = new java.text.SimpleDateFormat("HH:mm:ss.SSS").format(new Date());
        String formattedMessage = "[" + timestamp + "] " + msg;
        deliveredMessages.add(formattedMessage);
        System.out.println("\n>>> MENSAGEM ENTREGUE: " + formattedMessage);
    }

    /**
     * Efetua a liberacão e envio físico via rede de pacotes previamente retidos na fila de saída.
     */
    private void sendPendingOutgoingMessages() {
        List<DelayedPacket> pending = causalMulticast.getOutgoingDelayedQueue();

        System.out.println("\n===== MENSAGENS RETIDAS NO EMISSOR =====");
        if (pending.isEmpty()) {
            System.out.println("Nenhuma mensagem pendente de envio.");
            return;
        }

        System.out.println("Disparando " + pending.size() + " pacote(s) retido(s)...");

        // Copia e limpa para evitar concorrência durante a iteracão de envio
        List<DelayedPacket> toSend = new ArrayList<>(pending);
        pending.clear();

        for (DelayedPacket packet : toSend) {
            causalMulticast.sendUnicastDirect(packet.getPeerId(), packet.getMessage());
            System.out.println("[UDP] Pacote retido liberado com sucesso para -> " + packet.getPeerId());
        }
    }
}