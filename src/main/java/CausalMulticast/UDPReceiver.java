package CausalMulticast;

import java.net.DatagramPacket;
import java.net.DatagramSocket;

public class UDPReceiver implements Runnable {
    private final int localPort;
    private final CausalMulticast causalMulticast;
    private DatagramSocket socket;
    private volatile boolean running = true;
    // 65507 é o tamanho máximo de payload UDP (65535 - 20 bytes de header IPv4 - 8 bytes de header UDP)
    private static final int BUFFER_SIZE = 65507;
    private DelayQueue delayQueue;

    public UDPReceiver(int localPort, CausalMulticast causalMulticast, DelayQueue delayQueue) {
        this.localPort = localPort;
        this.causalMulticast = causalMulticast;
        this.delayQueue = delayQueue;
    }

    @Override
    public void run(){
        try{
            socket = new DatagramSocket(localPort);
            System.out.println("[RECEIVER] Escutando porta "+ localPort);

            while (running) {
                byte[] buffer = new byte[BUFFER_SIZE];
                DatagramPacket packet = new DatagramPacket(buffer, buffer.length);

                socket.receive(packet);

                try{
                    byte[] data = new byte[packet.getLength()];
                    System.arraycopy(packet.getData(), 0, data, 0, packet.getLength());

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
                }catch (Exception e){
                    System.err.println("[UDP RECEIVER ERRO] Erro ao desserializar mensagem: " + e.getMessage());
                }
            }
        } catch (Exception e) {
            System.err.println("[UDP RECEIVER FALHOU] " + e.getMessage());
        } finally {
            if (socket != null && !socket.isClosed()) {
                socket.close();
            }
        }
    }

    public void stop() {
        running = false;
        if (socket != null && !socket.isClosed()) {
            socket.close();
        }
    }
}
