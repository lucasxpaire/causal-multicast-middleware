package CausalMulticast;

import java.util.*;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * Interface de Console Interativa e Ponto de Entrada (Application Node) do sistema.
 * Implementa a interface {@link ICausalMulticast} para atuar como o cliente que
 * consome as mensagens entregues de forma ordenada pelo middleware. Gerencia o ciclo
 * de vida dos serviços de rede subjacentes (Descoberta, Envio e Recepção).
 *  @author Seu Nome
 * @version 1.0
 */
public class App implements ICausalMulticast {

    /** Instância principal do middleware de ordenação causal associada a este nó. */
    private CausalMulticast causalMulticast;

    /** Estrutura responsável pela simulação de atraso artificial na entrega de pacotes de rede. */
    private DelayQueue delayQueue;

    /** Serviço periódico baseado em Multicast UDP para anúncio e descoberta automática de peers na rede local. */
    private DiscoveryService discoveryService;

    /** Thread encarregada de escutar a porta UDP local de forma ininterrupta para receber mensagens físicas. */
    private UDPReceiver udpReceiver;

    /** Identificador exclusivo formatado para o nó local no padrão "IP:Porta". */
    private String localId;

    /** Porta de comunicação Unicast UDP configurada para este processo. */
    private int localPort;

    /** Lista segura para threads que mantém o histórico de strings já entregues em ordem lógica e exibidas na console. */
    private final List<String> deliveredMessages = new CopyOnWriteArrayList<>();

    /**
     * Ponto de entrada padrão da aplicação (Main Method).
     * Solicita as configurações de rede iniciais via console, instancia o nó do sistema,
     * inicializa os daemons em background e aciona o laço do menu interativo.
     *  @param args Argumentos de linha de comando (não utilizados).
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
     * Construtor da Aplicação Cliente.
     * Mapeia os dados do nó local e realiza a instanciação acoplada do middleware
     * injetando a si mesmo como o receptor final das mensagens ordenadas causais.
     *  @param ip Endereço IP local que será associado ao nó.
     * @param port Porta de comunicação local que será aberta para o nó.
     */
    public App(String ip, int port) {
        this.localId = ip + ":" + port;
        this.localPort = port;
        this.causalMulticast = new CausalMulticast(ip, port, this);
        this.delayQueue = new DelayQueue(causalMulticast);
    }

    /**
     * Inicializa os serviços de rede executados concorrentemente.
     * Instancia e dispara as threads assíncronas do {@link UDPReceiver} e do
     * {@link DiscoveryService} configurando-as para manter a execução ativa do sistema.
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
     * Loop principal que exibe e gerencia o menu de linha de comando interativo do nó.
     *  @param scanner O leitor de entrada padrão do sistema associado à console.
     */
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

    /**
     * Captura uma mensagem em texto digitada pelo operador na console e repassa
     * para o método {@code mcsend} do middleware para propagação confiável e causal.
     *  @param scanner O leitor de entrada padrão associado à console.
     */
    private void sendMessage(Scanner scanner) {
        System.out.print("Digite a mensagem: ");
        String message = scanner.nextLine().trim();

        if (!message.isEmpty()) {
            causalMulticast.mcsend(message, this);
            System.out.println("[APP] Mensagem enviada!");
        }
    }

    /**
     * Imprime na saída padrão a tabela textual formatada correspondente ao
     * estado atual de conhecimento global mantido pela Matriz de Relógios do middleware.
     */
    private void viewMatrixClock() {
        System.out.println(causalMulticast.getMatrixClockState());
    }

    /**
     * Imprime na saída padrão a lista de mensagens recebidas que ainda encontram-se
     * represadas no buffer do middleware aguardando sua validação causal ou estabilização.
     */
    private void viewMessageBuffer() {
        System.out.println(causalMulticast.getBufferState());
    }

    /**
     * Permite ao operador selecionar um dos peers descobertos automaticamente e configurar
     * um tempo de retenção artificial (atraso de rede em ms) para os pacotes oriundos dele.
     *  @param scanner O leitor de entrada padrão associado à console.
     */
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

    /**
     * Imprime na console todo o histórico de mensagens de texto limpas que o algoritmo causal
     * já validou, ordenou e liberou com sucesso para a aplicação.
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
     * Exibe na console os endereços de rede dos peers externos e ativos descobertos na rede local
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
     * Realiza o desligamento limpo e ordenado do nó local.
     * Encerra os loops dos sockets receptores e de descoberta, além de desligar os
     * pools de agendamento de tarefas da fila de atrasos para evitar vazamentos de memória.
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
     * Implementação do método de callback do contrato {@link ICausalMulticast}.
     * Este método é invocado assincronamente pela camada do middleware assim que uma
     * mensagem atende a todos os critérios de precedência causal, registrando o timestamp de liberação.
     *  @param msg Conteúdo textual puro da mensagem que foi validada e entregue.
     */
    @Override
    public void deliver(String msg) {
        String timestamp = new java.text.SimpleDateFormat("HH:mm:ss.SSS").format(new Date());
        String formattedMessage = "[" + timestamp + "] " + msg;
        deliveredMessages.add(formattedMessage);
        System.out.println("\n>>> MENSAGEM ENTREGUE: " + formattedMessage);
    }
}