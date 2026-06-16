package CausalMulticast;

import com.google.gson.Gson;

/**
 * Classe utilitária responsável pela serialização e desserialização de mensagens.
 * Fornece métodos estáticos para converter instâncias de {@link BufferedMessage} em
 * strings JSON e arrays de bytes (e vice-versa).
 * @author -
 * @version 1.0
 */
public class MessageSerializer {

    /** Instância compartilhada e thread-safe do Gson para manipulação eficiente de JSON. */
    private static final Gson gson = new Gson();

    /**
     * Construtor privado para impedir a instanciação desnecessária desta classe utilitária.
     */
    private MessageSerializer() {
    }

    /**
     * Serializa um objeto do tipo {@link BufferedMessage} para o formato de texto JSON.
     * @param message A instância da mensagem contendo carga útil e metadados.
     * @return Uma {@link String} contendo a representação textual JSON do objeto.
     */
    public static String serialize(BufferedMessage message) {
        return gson.toJson(message);
    }

    /**
     * Desserializa uma string formatada em JSON de volta para um objeto Java {@link BufferedMessage}.
     * @param json A string textual em formato JSON que representa a mensagem.
     * @return Uma nova instância de {@link BufferedMessage} preenchida com os dados originais.
     */
    public static BufferedMessage deserialize(String json) {
        return gson.fromJson(json, BufferedMessage.class);
    }

    /**
     * Converte uma instância de {@link BufferedMessage} diretamente em um array de bytes.
     * @param message A instância da mensagem que será convertida.
     * @return Um array de bytes pronto para ser encapsulado e transmitido.
     */
    public static byte[] toBytes(BufferedMessage message) {
        return serialize(message).getBytes(java.nio.charset.StandardCharsets.UTF_8);
    }

    /**
     * Reconstrói uma instância de {@link BufferedMessage} a partir de um array de bytes bruto.
     * @param data O array de bytes lido diretamente do buffer de recepção.
     * @return A instância de {@link BufferedMessage} com sua integridade restaurada.
     */
    public static BufferedMessage fromBytes(byte[] data) {
        String json = new String(data, java.nio.charset.StandardCharsets.UTF_8);
        return deserialize(json);
    }
}