package CausalMulticast;

import java.net.DatagramPacket;
import java.net.DatagramSocket;

/**
 * Receptor assíncrono baseado em UDP executado em uma Thread dedicada (Daemon/Worker).
 * Implementa a interface {@link Runnable} para escutar continuamente uma porta de rede
 * local Unicast. Captura pacotes brutos do sistema operacional, reconstrói os objetos
 * estruturados de mensagem através do {@link MessageSerializer} e os encaminha de forma
 * condicional para a fila de atraso artificial ou diretamente para o núcleo do middleware.
 *  @author Seu Nome
 * @version 1.0
 */
public class UDPReceiver implements Runnable {

    /** A porta local UDP associada a este socket para recepção de pacotes. */
    private final int localPort;

    /** Referência ao núcleo do middleware de ordenação causal para entrega direta de dados. */
    private final CausalMulticast causalMulticast;

    /** O socket Unicast Datagram nativo do Java utilizado para a escuta da porta. */
    private DatagramSocket socket;

    /** Flag de controle de execução com semântica de memória volátil para interrupção segura entre threads. */
    private volatile boolean running = true;

    /**  Tamanho máximo permitido para o payload de dados de um datagrama UDP clássico.
     * Calculado como: 65535 (Tamanho máx. IP) - 20 (Header IPv4) - 8 (Header UDP) = 65507 bytes.
     */
    private static final int BUFFER_SIZE = 65507;

    /** Referência ao gerenciador de atrasos artificiais de entrega do sistema. */
    private DelayQueue delayQueue;

    /**
     * Construtor do Receptor UDP.
     *  @param localPort Porta lógica de rede que o nó ocupará.
     * @param causalMulticast Instância ativa do middleware de ordenação causal.
     * @param delayQueue Instância do gerenciador de filas de atraso para peers conhecidos.
     */
    public UDPReceiver(int localPort, CausalMulticast causalMulticast, DelayQueue delayQueue) {
        this.localPort = localPort;
        this.causalMulticast = causalMulticast;
        this.delayQueue = delayQueue;
    }

    /**
     * Loop principal de processamento executado de forma concorrente em background.
     * Aloca o socket na porta especificada e entra em um ciclo de bloqueio contínuo aguardando
     * a chegada de pacotes físicos. Realiza o recorte preciso do buffer de bytes e delega
     * a classificação causal ou o agendamento de atraso.
     */
    @Override
    public void run() {
        try {
            socket = new DatagramSocket(localPort);
            System.out.println("[RECEIVER] Escutando porta " + localPort);

            while (running) {
                byte[] buffer = new byte[BUFFER_SIZE];
                DatagramPacket packet = new DatagramPacket(buffer, buffer.length);

                // Bloqueia a thread até que um pacote seja recebido na porta de rede
                socket.receive(packet);

                try {
                    byte[] data = new byte[packet.getLength()];
                    System.arraycopy(packet.getData(), 0, data, 0, packet.getLength());

                    // Reconstrói a mensagem a partir do payload em JSON/Bytes
                    BufferedMessage message = MessageSerializer.fromBytes(data);
                    String senderId = message.getSenderId();
                    long delay = delayQueue.getPeerDelay(senderId);

                    System.out.println("[RECEIVER] Mensagem vinda de " + packet.getAddress().getHostAddress() + ":" + packet.getPort());

                    if (delay > 0) {
                        delayQueue.addDelayedMessage(senderId, message, delay);
                        System.out.println("[DELAY] Atraso de " + delay + "ms aplicado à mensagem de " + senderId);
                    } else {
                        causalMulticast.onMessageReceived(message);
                    }
                } catch (Exception e) {
                    System.err.println("[UDP RECEIVER ERRO] Erro ao desserializar mensagem: " + e.getMessage());
                }
            }
        } catch (Exception e) {
            System.err.println("[UDP RECEIVER FALHOU] " + e.getMessage());
        } finally {
            // Garante a liberação dos recursos do sistema operacional ao sair do loop
            if (socket != null && !socket.isClosed()) {
                socket.close();
            }
        }
    }

    /**
     * Interrompe a execução do laço de recepção de forma limpa.
     * Modifica a flag de estado e força o fechamento imediato do {@link DatagramSocket}
     * subjacente para desbloquear a operação síncrona {@code socket.receive(packet)} por meio de uma exceção controlada.
     */
    public void stop() {
        running = false;
        if (socket != null && !socket.isClosed()) {
            socket.close();
        }
    }
}