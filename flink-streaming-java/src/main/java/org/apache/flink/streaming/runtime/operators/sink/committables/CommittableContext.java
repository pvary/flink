package org.apache.flink.streaming.runtime.operators.sink.committables;

import org.apache.flink.metrics.groups.SinkCommitterMetricGroup;

import javax.annotation.Nullable;

public class CommittableContext {
    @Nullable
    private final Long checkpointId;
    private final int subtaskId;
    private SinkCommitterMetricGroup metricGroup;

    public CommittableContext(
            @Nullable Long checkpointId,
            int subtaskId,
            SinkCommitterMetricGroup metricGroup) {
        this.checkpointId = checkpointId;
        this.subtaskId = subtaskId;
        this.metricGroup = metricGroup;
    }

    @Nullable
    Long getCheckpointId() {
        return checkpointId;
    }

    int getSubtaskId() {
        return subtaskId;
    }

    SinkCommitterMetricGroup getMetricGroup() {
        return metricGroup;
    }
}
