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
import com.alibaba.fluss.client.table.Table;
import com.alibaba.fluss.client.table.writer.AppendWriter;
import com.alibaba.fluss.config.Configuration;
import com.alibaba.fluss.memory.MemorySegment;
import com.alibaba.fluss.metadata.TablePath;
import com.alibaba.fluss.row.InternalRow;
import com.alibaba.fluss.row.indexed.IndexedRow;
import com.alibaba.fluss.types.DataType;
import com.alibaba.fluss.utils.IOUtils;
import io.openmessaging.benchmark.driver.BenchmarkProducer;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Fluss benchmark producer. */
public class FlussBenchmarkProducer implements BenchmarkProducer {

    private static final Logger LOG = LoggerFactory.getLogger(FlussBenchmarkConsumer.class);

    private final AppendWriter appendWriter;
    private final DataType[] fieldTypes;
    private Connection connection;
    private final Table table;

    public FlussBenchmarkProducer(Configuration conf, String topic) {
        this.connection = ConnectionFactory.createConnection(conf);
        this.table = connection.getTable(new TablePath(DEFAULT_DATABASE_NAME, topic));
        this.appendWriter = table.getAppendWriter();
        this.fieldTypes =
                table.getDescriptor().getSchema().toRowType().getChildren().toArray(new DataType[0]);
    }

    @Override
    public CompletableFuture<Void> sendAsync(Optional<String> key, byte[] payload) {
        return appendWriter.append(toInternalRow(payload));
    }

    @Override
    public void close() throws Exception {
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

    private InternalRow toInternalRow(byte[] payload) {
        IndexedRow row = new IndexedRow(fieldTypes);
        row.pointTo(MemorySegment.wrap(payload), 0, payload.length);
        return row;
    }
}
