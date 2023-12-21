package mn.tqt.presentation;

import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;

public record Query(String startDate,
                    String endDate,
                    String kafkaEndpoint,
                    String kafkaTopic,
                    String schemaRegistry,
                    QuerySchema schema) {

    public OffsetDateTime typedStartDate() {
        return stringToOffsetDatetime(startDate);
    }

    public java.util.List<LinkedList<String>> schemaAsListOfQueues() {
        return schema.schema().stream()
                .map(entry -> new LinkedList<>(List.of(entry.split("\\.")))).toList();
    }

    public OffsetDateTime typedEndDate() {
        return stringToOffsetDatetime(endDate);
    }

    private static OffsetDateTime stringToOffsetDatetime(String dateString) {
        var parts = new LinkedList<>(Arrays.stream(dateString.split("-")).toList());

        return OffsetDateTime.of(
                !parts.isEmpty() ? Integer.parseInt(parts.pop()) : 2021,
                !parts.isEmpty() ? Integer.parseInt(parts.pop()) : 1,
                !parts.isEmpty() ? Integer.parseInt(parts.pop()) : 1,
                !parts.isEmpty() ? Integer.parseInt(parts.pop()) : 0,
                !parts.isEmpty() ? Integer.parseInt(parts.pop()) : 0,
                !parts.isEmpty() ? Integer.parseInt(parts.pop()) : 0,
                !parts.isEmpty() ? Integer.parseInt(parts.pop()) : 0,
                ZoneOffset.UTC);

    }
}
