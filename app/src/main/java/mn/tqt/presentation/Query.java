package mn.tqt.presentation;

public record Query(String startDate,
                    String endDate,
                    String kafkaEndpoint,
                    String kafkaTopic,
                    String schemaRegistry,
                    QuerySchema schema) {

    public java.time.OffsetDateTime typedStartDate() {
        return stringToOffsetDatetime(startDate);
    }

    public java.util.List<java.util.LinkedList<String>> schemaAsListOfQueues() {
        return schema.schema().stream()
                .map(entry -> new java.util.LinkedList<>(java.util.List.of(entry.split("\\.")))).toList();
    }

    public java.time.OffsetDateTime typedEndDate() {
        return stringToOffsetDatetime(endDate);
    }

    private static java.time.OffsetDateTime stringToOffsetDatetime(String dateString) {
        var parts = new java.util.LinkedList<>(java.util.Arrays.stream(dateString.split("-")).toList());

        return java.time.OffsetDateTime.of(
                !parts.isEmpty() ? Integer.parseInt(parts.pop()) : 2021,
                !parts.isEmpty() ? Integer.parseInt(parts.pop()) : 1,
                !parts.isEmpty() ? Integer.parseInt(parts.pop()) : 1,
                !parts.isEmpty() ? Integer.parseInt(parts.pop()) : 0,
                !parts.isEmpty() ? Integer.parseInt(parts.pop()) : 0,
                !parts.isEmpty() ? Integer.parseInt(parts.pop()) : 0,
                !parts.isEmpty() ? Integer.parseInt(parts.pop()) : 0,
                java.time.ZoneOffset.UTC);

    }
}
