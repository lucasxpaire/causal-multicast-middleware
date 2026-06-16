package CausalMulticast;

import java.io.IOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;

public class UDPSender {
    private DatagramSocket socket;

    public UDPSender() throws IOException{
        this.socket = new DatagramSocket();
    }

    public void sendMessage(String receiverAddress, int receiverPort, BufferedMessage message){
        try{
            byte[] data = MessageSerializer.toBytes(message);
            InetAddress address = InetAddress.getByName(receiverAddress);
            DatagramPacket packet = new DatagramPacket(data, data.length, address, receiverPort);
            socket.send(packet);
            System.out.println("[UDP] Mensagem enviada para " + receiverAddress + ":" + receiverPort);
        } catch (Exception e) {
            System.err.println("[UDP ERROR] Erro ao enviar mensagem: " + e.getMessage());
        }
    }
    public void close() {
        if (socket != null && !socket.isClosed()) {
            socket.close();
        }
    }
}

