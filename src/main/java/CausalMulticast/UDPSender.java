package CausalMulticast;

import java.io.IOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;

/**
 * Emissor de dados baseado em sockets Unicast UDP.
 * Esta classe encapsula as operações de rede de baixo nível necessárias para despachar
 * instâncias de {@link BufferedMessage} para destinos específicos na rede. O objeto é
 * convertido em array de bytes via {@link MessageSerializer} antes do encapsulamento físico
 * no datagrama.
 *  @author Seu Nome
 * @version 1.0
 */
public class UDPSender {

    /** O socket Datagram nativo do Java utilizado para a saída de pacotes de dados. */
    private DatagramSocket socket;

    /**
     * Construtor padrão do emissor UDP.
     * Inicializa um novo socket de datagrama alocado automaticamente em uma porta livre
     * pelo sistema operacional.
     *  @throws IOException Se ocorrer um erro de E/S ao abrir ou inicializar o socket.
     */
    public UDPSender() throws IOException {
        this.socket = new DatagramSocket();
    }

    /**
     * Transmite uma mensagem encapsulada para um nó remoto específico utilizando o protocolo UDP.
     * Transforma o objeto estruturado em um fluxo binário e realiza o endereçamento IP/Porta.
     *  @param receiverAddress O endereço IP ou Hostname de destino (ex: "192.168.1.101").
     * @param receiverPort A porta lógica do socket receptor no nó de destino.
     * @param message A instância da mensagem contendo o payload e os relógios causais.
     */
    public void sendMessage(String receiverAddress, int receiverPort, BufferedMessage message) {
        try {
            // Converte o envelope estruturado da mensagem em um array de bytes brutos (JSON em UTF-8)
            byte[] data = MessageSerializer.toBytes(message);

            // Resolve o hostname/IP textual fornecido em um objeto InetAddress nativo do Java
            InetAddress address = InetAddress.getByName(receiverAddress);

            // Monta o pacote de dados UDP encapsulando o buffer binário e os metadados de destino
            DatagramPacket packet = new DatagramPacket(data, data.length, address, receiverPort);

            // Despacha o pacote de forma assíncrona (não-confiável por padrão do protocolo) para a rede
            socket.send(packet);
            System.out.println("[UDP] Mensagem enviada para " + receiverAddress + ":" + receiverPort);
        } catch (Exception e) {
            System.err.println("[UDP ERROR] Erro ao enviar mensagem: " + e.getMessage());
        }
    }

    /**
     * Fecha e libera o socket UDP subjacente.
     * Interrompe qualquer alocação de canal pendente no sistema operacional. Deve ser
     * acionado durante os procedimentos de desligamento controlado da aplicação.
     */
    public void close() {
        if (socket != null && !socket.isClosed()) {
            socket.close();
        }
    }
}