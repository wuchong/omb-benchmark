/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.openmessaging.benchmark.driver.fluss;

import static com.alibaba.fluss.utils.function.ThrowingConsumer.unchecked;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.alibaba.fluss.client.Connection;
import com.alibaba.fluss.client.ConnectionFactory;
import com.alibaba.fluss.client.admin.Admin;
import com.alibaba.fluss.client.table.Table;
import com.alibaba.fluss.config.ConfigOptions;
import com.alibaba.fluss.config.Configuration;
import com.alibaba.fluss.config.MemorySize;
import com.alibaba.fluss.metadata.LogFormat;
import com.alibaba.fluss.metadata.Schema;
import com.alibaba.fluss.metadata.TableDescriptor;
import com.alibaba.fluss.metadata.TablePath;
import com.alibaba.fluss.types.DataTypes;
import com.alibaba.fluss.utils.IOUtils;
import io.openmessaging.benchmark.driver.BenchmarkConsumer;
import io.openmessaging.benchmark.driver.BenchmarkDriver;
import io.openmessaging.benchmark.driver.BenchmarkProducer;
import io.openmessaging.benchmark.driver.ConsumerCallback;
import java.io.File;
import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import org.apache.bookkeeper.stats.StatsLogger;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Fluss benchmark driver. */
public class FlussBenchmarkDriver implements BenchmarkDriver {
    private static final Logger LOG = LoggerFactory.getLogger(FlussBenchmarkDriver.class);

    public static final String DEFAULT_DATABASE_NAME = "benchmarkDb";
    private static final ObjectWriter writer = new ObjectMapper().writerWithDefaultPrettyPrinter();
    private static final ObjectMapper mapper =
            new ObjectMapper(new YAMLFactory())
                    .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);

    private Configuration flussConfig;
    private Connection connection;
    private Admin flussAdmin;
    private int[] projectFields;
    private LogFormat logFormat;

    private final List<BenchmarkProducer> producers = Collections.synchronizedList(new ArrayList<>());
    private final List<BenchmarkConsumer> consumers = Collections.synchronizedList(new ArrayList<>());

    @Override
    public void initialize(File configurationFile, StatsLogger statsLogger) throws IOException {
        this.flussConfig = readConfig(configurationFile);
        this.connection = ConnectionFactory.createConnection(flussConfig);
        this.flussAdmin = connection.getAdmin();
    }

    @Override
    public String getTopicNamePrefix() {
        return "Fluss-Benchmark";
    }

    @Override
    public CompletableFuture<Void> createTopic(String topic, int partitions, String[] rowTypeString) {
        try {
            flussAdmin.createDatabase(DEFAULT_DATABASE_NAME, true).get();
        } catch (Exception e) {
            throw new RuntimeException("Exception occurs while creating Fluss database.", e);
        }

        Schema schema = createSchema(rowTypeString);
        TableDescriptor tableDescriptor =
                TableDescriptor.builder()
                        .schema(schema)
                        .logFormat(logFormat)
                        .distributedBy(partitions)
                        .build();
        return flussAdmin.createTable(
                new TablePath(DEFAULT_DATABASE_NAME, topic), tableDescriptor, false);
    }

    private Schema createSchema(String[] rowTypeString) {
        Schema.Builder builder = Schema.newBuilder();
        int i = 0;
        for (String rowType : rowTypeString) {
            String rowName = "row" + i;
            switch (rowType) {
                case "int":
                    builder.column(rowName, DataTypes.INT());
                    break;
                case "long":
                    builder.column(rowName, DataTypes.BIGINT());
                    break;
                case "string":
                    builder.column(rowName, DataTypes.STRING());
                    break;
                default:
                    throw new RuntimeException("Unsupported row type: " + rowType);
            }
            i++;
        }
        return builder.build();
    }

    @Override
    public CompletableFuture<BenchmarkProducer> createProducer(String topic) {
        BenchmarkProducer benchmarkProducer = new FlussBenchmarkProducer(flussConfig, topic);
        try {
            // Add to producer list to close later
            producers.add(benchmarkProducer);
            return CompletableFuture.completedFuture(benchmarkProducer);
        } catch (Throwable t) {
            CompletableFuture<BenchmarkProducer> future = new CompletableFuture<>();
            future.completeExceptionally(t);
            return future;
        }
    }

    @Override
    public CompletableFuture<BenchmarkConsumer> createConsumer(
            int id,
            int partitionsPerTopic,
            int partitionsPerSubscription,
            String topic,
            String subscriptionName,
            ConsumerCallback consumerCallback) {
        Table table = connection.getTable(new TablePath(DEFAULT_DATABASE_NAME, topic));
        List<Integer> subscriptionBuckets =
                calculateSubscriptionBuckets(id, partitionsPerTopic, partitionsPerSubscription);
        FlussBenchmarkConsumer consumer =
                new FlussBenchmarkConsumer(
                        flussConfig, topic, projectFields, subscriptionBuckets, consumerCallback);
        try {
            consumers.add(consumer);
            return CompletableFuture.completedFuture(consumer);
        } catch (Throwable t) {
            IOUtils.closeQuietly(table);
            CompletableFuture<BenchmarkConsumer> future = new CompletableFuture<>();
            future.completeExceptionally(t);
            return future;
        }
    }

    @Override
    public void close() {
        producers.forEach(unchecked(BenchmarkProducer::close));
        consumers.forEach(unchecked(BenchmarkConsumer::close));

        try {
            if (connection != null) {
                connection.close();
            }
        } catch (Exception e) {
            LOG.warn("Exception occurs while closing Fluss Connection.", e);
        }
        connection = null;
    }

    private List<Integer> calculateSubscriptionBuckets(
            int id, int partitionsPerTopic, int subscriptionsPerTopic) {
        List<Integer> buckets = new ArrayList<>();

        int partitionsPerSubscription = partitionsPerTopic / subscriptionsPerTopic;
        int extraPartitions = partitionsPerTopic % subscriptionsPerTopic;

        int startBucket;
        if (id < extraPartitions) {
            startBucket = id * (partitionsPerSubscription + 1);
        } else {
            startBucket = id * partitionsPerSubscription + extraPartitions;
        }

        int endBucket = startBucket + partitionsPerSubscription - 1;
        if (id < extraPartitions) {
            endBucket += 1;
        }

        for (int i = startBucket; i <= endBucket; i++) {
            buckets.add(i);
        }
        LOG.info(
                "id: {}, the subscriptionBuckets is {}, total buckets: {}, total consumers: {}",
                id,
                buckets,
                partitionsPerTopic,
                subscriptionsPerTopic);
        return buckets;
    }

    private Configuration readConfig(File configurationFile) throws IOException {
        FlussConfig flussConfig = mapper.readValue(configurationFile, FlussConfig.class);
        LOG.info("Fluss driver conf: {}", writer.writeValueAsString(flussConfig));

        Configuration conf = new Configuration();
        FlussClientConfig clientConfig = flussConfig.client;
        conf.setString(ConfigOptions.BOOTSTRAP_SERVERS.key(), clientConfig.bootstrapServers);
        conf.setString(ConfigOptions.CLIENT_WRITER_ACKS.key(), clientConfig.writerAcks);
        conf.set(
                ConfigOptions.CLIENT_WRITER_BATCH_SIZE, MemorySize.parse(clientConfig.writerBatchSize));
        conf.set(
                ConfigOptions.CLIENT_WRITER_BUFFER_MEMORY_SIZE,
                MemorySize.parse(clientConfig.writerBufferMemory));
        conf.set(
                ConfigOptions.CLIENT_WRITER_BATCH_TIMEOUT,
                Duration.ofMillis(clientConfig.writerBatchTimeoutMs));
        conf.setInt(ConfigOptions.NETTY_CLIENT_NUM_NETWORK_THREADS, clientConfig.clientNettyThreads);
        conf.set(ConfigOptions.LOG_FETCH_MAX_BYTES, MemorySize.parse(clientConfig.fetchMaxBytes));
        conf.set(ConfigOptions.CLIENT_SCANNER_REMOTE_LOG_PREFETCH_NUM, clientConfig.prefetchNum);
        conf.setBoolean(ConfigOptions.CLIENT_SCANNER_LOG_CHECK_CRC, clientConfig.isCheckCrc);
        //        conf.set(
        //                ConfigOptions.CLIENT_WRITER_MEMORY_SEGMENT_SIZE,
        // MemorySize.parse(clientConfig.pageSize));
        //        conf.setInt(ConfigOptions.CLIENT_WRITER_ARROW_BATCH_RECORD_NUM,
        // clientConfig.arrowRecordNum);

        if (clientConfig.logFormat.equals(LogFormat.INDEXED.toString())) {
            this.logFormat = LogFormat.INDEXED;
        } else {
            this.logFormat = LogFormat.ARROW;
        }

        String projectFieldString = clientConfig.projectFields;
        if (projectFieldString.equals("all")) {
            projectFields = new int[0];
        } else {
            String[] fields = projectFieldString.split("/");
            projectFields = new int[fields.length];
            for (int i = 0; i < fields.length; i++) {
                projectFields[i] = Integer.parseInt(fields[i]);
            }
        }

        return conf;
    }
}
