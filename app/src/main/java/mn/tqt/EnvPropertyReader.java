package mn.tqt;

public class EnvPropertyReader {

    public String kafkaServerUrl() {
        String kafkaServerUrl = System.getenv("KAFKA_SERVER_URL");
        return kafkaServerUrl != null ? kafkaServerUrl : "localhost:9094";
    }

    public String schemaRegistryUrl() {
        String kafkaServerUrl = System.getenv("SCHEMA_REGISTRY_URL");
        return kafkaServerUrl != null ? kafkaServerUrl : "localhost:8083";
    }
}
