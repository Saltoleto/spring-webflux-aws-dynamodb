import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.Map;

public class Base64MapUtil {

    public static String encodeMapToBase64(Map<String, Attributes> dataMap) {
        ObjectMapper objectMapper = new ObjectMapper();
        try {
            String json = objectMapper.writeValueAsString(dataMap);
            byte[] encodedBytes = Base64.getEncoder().encode(json.getBytes(StandardCharsets.UTF_8));
            return new String(encodedBytes, StandardCharsets.UTF_8);
        } catch (JsonProcessingException e) {
            e.printStackTrace();
            return null;
        }
    }

    public static Map<String, Attributes> decodeBase64ToMap(String base64Encoded) {
        byte[] decodedBytes = Base64.getDecoder().decode(base64Encoded.getBytes(StandardCharsets.UTF_8));
        String json = new String(decodedBytes, StandardCharsets.UTF_8);
        
        ObjectMapper objectMapper = new ObjectMapper();
        try {
            TypeReference<Map<String, Attributes>> mapType = new TypeReference<Map<String, Attributes>>() {};
            return objectMapper.readValue(json, mapType);
        } catch (JsonProcessingException e) {
            e.printStackTrace();
            return null;
        }
    }
}
