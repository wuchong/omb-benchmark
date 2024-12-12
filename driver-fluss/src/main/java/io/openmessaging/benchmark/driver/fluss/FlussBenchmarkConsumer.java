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

import static io.openmessaging.benchmark.driver.fluss.FlussBenchmarkDriver.DEFAULT_DATABASE_NAME;

import com.alibaba.fluss.client.Connection;
import com.alibaba.fluss.client.ConnectionFactory;
import com.alibaba.fluss.client.scanner.log.LogScan;
import com.alibaba.fluss.client.scanner.log.LogScanner;
import com.alibaba.fluss.client.scanner.log.ScanRecords;
import com.alibaba.fluss.client.table.Table;
import com.alibaba.fluss.config.Configuration;
import com.alibaba.fluss.metadata.TablePath;
import com.alibaba.fluss.row.BinaryString;
import com.alibaba.fluss.row.InternalRow;
import com.alibaba.fluss.types.DataType;
import com.alibaba.fluss.types.DataTypeRoot;
import com.alibaba.fluss.types.RowType;
import com.alibaba.fluss.utils.IOUtils;
import io.openmessaging.benchmark.driver.BenchmarkConsumer;
import io.openmessaging.benchmark.driver.ConsumerCallback;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Fluss benchmark consumer. */
public class FlussBenchmarkConsumer implements BenchmarkConsumer {

    private static final Logger LOG = LoggerFactory.getLogger(FlussBenchmarkConsumer.class);

    private Connection connection;
    private final Table table;
    private final LogScanner logScanner;
    private final int fieldLength;
    private final InternalRow.FieldGetter[] flussFieldGetters;
    private final ExecutorService executor;
    private final Future<?> consumerTask;
    private volatile boolean closing = false;

    public FlussBenchmarkConsumer(
            Configuration conf,
            String topic,
            int[] projectFields,
            List<Integer> subscriptionBuckets,
            ConsumerCallback callback) {
        this(conf, topic, projectFields, subscriptionBuckets, callback, 100L);
    }

    public FlussBenchmarkConsumer(
            Configuration conf,
            String topic,
            int[] projectFields,
            List<Integer> subscriptionBuckets,
            ConsumerCallback callback,
            long pollTimeoutMs) {
        this.connection = ConnectionFactory.createConnection(conf);
        this.table = connection.getTable(new TablePath(DEFAULT_DATABASE_NAME, topic));
        RowType originRowType = table.getDescriptor().getSchema().toRowType();
        this.logScanner = createLogScanner(table, projectFields, subscriptionBuckets);
        RowType projectRowType = originRowType.project(projectFields);
        this.fieldLength = projectRowType.getFieldCount();
        this.flussFieldGetters = new InternalRow.FieldGetter[fieldLength];
        for (int i = 0; i < fieldLength; i++) {
            flussFieldGetters[i] = InternalRow.createFieldGetter(projectRowType.getTypeAt(i), i);
        }

        this.executor = Executors.newSingleThreadExecutor();
        this.consumerTask =
                this.executor.submit(
                        () -> {
                            while (!closing) {
                                try {
                                    ScanRecords records = logScanner.poll(Duration.ofMillis(pollTimeoutMs));
                                    records.forEach(
                                            record -> {
                                                InternalRow row = record.getRow();
                                                int sizeInBytes = 0;
                                                long timeStamp = (long) flussFieldGetters[0].getFieldOrNull(row);
                                                for (int i = 1; i < fieldLength; i++) {
                                                    Object value = flussFieldGetters[i].getFieldOrNull(row);
                                                    sizeInBytes += objectSizes(value, projectRowType.getTypeAt(i));
                                                }
                                                callback.messageReceived(new byte[sizeInBytes], timeStamp);
                                            });
                                } catch (Exception e) {
                                    LOG.error("exception occur while consuming message", e);
                                }
                            }
                        });
    }

    private LogScanner createLogScanner(
            Table table, int[] projectFields, List<Integer> subscriptionBuckets) {
        LogScanner logScanner;
        if (projectFields.length == 0) {
            logScanner = table.getLogScanner(new LogScan());
        } else {
            logScanner = table.getLogScanner(new LogScan().withProjectedFields(projectFields));
        }
        for (int subscriptionBucket : subscriptionBuckets) {
            logScanner.subscribeFromBeginning(subscriptionBucket);
        }

        return logScanner;
    }

    private int objectSizes(Object obj, DataType type) {
        DataTypeRoot typeRoot = type.getTypeRoot();
        if (typeRoot == DataTypeRoot.INTEGER) {
            return 4;
        } else if (typeRoot == DataTypeRoot.BIGINT) {
            return 8;
        } else if (typeRoot == DataTypeRoot.STRING) {
            return ((BinaryString) obj).getSizeInBytes();
        } else {
            throw new UnsupportedOperationException("Unsupported type: " + typeRoot);
        }
    }

    @Override
    public void close() throws Exception {
        closing = true;
        executor.shutdown();
        consumerTask.get();
        logScanner.close();

        IOUtils.closeQuietly(table);

        try {
            if (connection != null) {
                connection.close();
            }
        } catch (Exception e) {
            LOG.warn("Exception occurs while closing Fluss Connection.", e);
        }
        connection = null;
    }
}
