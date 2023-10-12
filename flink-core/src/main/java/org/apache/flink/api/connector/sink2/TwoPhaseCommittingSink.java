/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.api.connector.sink2;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.api.common.JobID;
import org.apache.flink.api.connector.sink2.StatefulSink.StatefulSinkWriter;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.metrics.groups.SinkCommitterMetricGroup;

import java.io.IOException;
import java.util.Collection;
import java.util.OptionalLong;

/**
 * A {@link Sink} for exactly-once semantics using a two-phase commit protocol. The {@link Sink}
 * consists of a {@link SinkWriter} that performs the precommits and a {@link Committer} that
 * actually commits the data. To facilitate the separation the {@link SinkWriter} creates
 * <i>committables</i> on checkpoint or end of input and the sends it to the {@link Committer}.
 *
 * <p>The {@link TwoPhaseCommittingSink} needs to be serializable. All configuration should be
 * validated eagerly. The respective sink writers and committers are transient and will only be
 * created in the subtasks on the taskmanagers.
 *
 * @param <InputT> The type of the sink's input
 * @param <CommT> The type of the committables.
 */
@PublicEvolving
public interface TwoPhaseCommittingSink<InputT, CommT> extends Sink<InputT> {

    /**
     * Creates a {@link PrecommittingSinkWriter} that creates committables on checkpoint or end of
     * input.
     *
     * @param context the runtime context.
     * @return A sink writer for the two-phase commit protocol.
     * @throws IOException for any failure during creation.
     */
    PrecommittingSinkWriter<InputT, CommT> createWriter(InitContext context) throws IOException;

    /**
     * Creates a {@link Committer} that permanently makes the previously written data visible
     * through {@link Committer#commit(Collection)}.
     *
     * @return A committer for the two-phase commit protocol.
     * @throws IOException for any failure during creation.
     * @deprecated Please use {@link #createCommitter(CommitterInitContext)}
     */
    @Deprecated
    default Committer<CommT> createCommitter() throws IOException {
        throw new UnsupportedOperationException(
                "Deprecated, please use createCommitter(CommitterInitContext)");
    }

    /**
     * Creates a {@link Committer} that permanently makes the previously written data visible
     * through {@link Committer#commit(Collection)}.
     *
     * @param context The context information for the committer initialization.
     * @return A committer for the two-phase commit protocol.
     * @throws IOException for any failure during creation.
     */
    default Committer<CommT> createCommitter(CommitterInitContext context) throws IOException {
        return createCommitter();
    }

    /** Returns the serializer of the committable type. */
    SimpleVersionedSerializer<CommT> getCommittableSerializer();

    /** A {@link SinkWriter} that performs the first part of a two-phase commit protocol. */
    @PublicEvolving
    interface PrecommittingSinkWriter<InputT, CommT> extends SinkWriter<InputT> {
        /**
         * Prepares for a commit.
         *
         * <p>This method will be called after {@link #flush(boolean)} and before {@link
         * StatefulSinkWriter#snapshotState(long)}.
         *
         * @return The data to commit as the second step of the two-phase commit protocol.
         * @throws IOException if fail to prepare for a commit.
         */
        Collection<CommT> prepareCommit() throws IOException, InterruptedException;
    }

    /** The interface exposes some runtime info for creating a {@link Committer}. */
    @PublicEvolving
    interface CommitterInitContext {
        /**
         * The first checkpoint id when an application is started and not recovered from a
         * previously taken checkpoint or savepoint.
         */
        long INITIAL_CHECKPOINT_ID = 1;

        /** @return The id of task where the committer is running. */
        int getSubtaskId();

        /** @return The number of parallel committer tasks. */
        int getNumberOfParallelSubtasks();

        /**
         * Gets the attempt number of this parallel subtask. First attempt is numbered 0.
         *
         * @return Attempt number of the subtask.
         */
        int getAttemptNumber();

        /** @return The metric group this committer belongs to. */
        SinkCommitterMetricGroup metricGroup();

        /**
         * Returns id of the restored checkpoint, if state was restored from the snapshot of a
         * previous execution.
         */
        OptionalLong getRestoredCheckpointId();

        /**
         * The ID of the current job. Note that Job ID can change in particular upon manual restart.
         * The returned ID should NOT be used for any job management tasks.
         */
        JobID getJobId();
    }
}
