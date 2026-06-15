package CausalMulticast;

import com.google.gson.Gson;

public class MessageSerializer {
    private static final Gson gson = new Gson();

    public static String serialize (BufferedMessage message){
        return gson.toJson(message);
    }

    public static BufferedMessage deserialize(String json){
        return gson.fromJson(json, BufferedMessage.class);
    }

    public static byte[] toBytes(BufferedMessage message){
        return serialize(message).getBytes(java.nio.charset.StandardCharsets.UTF_8);
    }

    public static BufferedMessage fromBytes(byte[] data){
        String json = new String(data, java.nio.charset.StandardCharsets.UTF_8);
        return deserialize(json);
    }
}