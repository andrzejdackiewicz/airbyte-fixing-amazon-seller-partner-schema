package io.airbyte.integrations.destination_performance;

import java.util.ArrayList;
import java.util.List;
import static org.junit.Assert.assertNotEquals;
import org.junit.jupiter.api.Test;

class PerformanceHarnessTest {

    @Test
    public void testRandomStreamName() {
        final List<String> streamNames = new ArrayList<>();
        final int duplicateFactor = 1000;
        // Keep this number high to avoid statistical collisions. Alternative was to consider chi-squared
        for (int i = 1; i <= duplicateFactor; i++) {
            streamNames.add("stream" + i);
        }
        final String streamName1 = PerformanceHarness.getStreamName(streamNames);
        final String streamName2 = PerformanceHarness.getStreamName(streamNames);
        assertNotEquals(streamName1, streamName2);
    }
}