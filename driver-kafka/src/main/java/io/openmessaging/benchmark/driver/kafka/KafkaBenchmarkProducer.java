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
package io.openmessaging.benchmark.driver.kafka;


import io.openmessaging.benchmark.driver.BenchmarkProducer;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

public class KafkaBenchmarkProducer implements BenchmarkProducer {

    private final Producer<String, byte[]> producer;
    private final String topic;

    public KafkaBenchmarkProducer(Producer<String, byte[]> producer, String topic) {
        this.producer = producer;
        this.topic = topic;
    }

    @Override
    public CompletableFuture<Void> sendAsync(Optional<String> key, byte[] payload) {
        ProducerRecord<String, byte[]> record = new ProducerRecord<>(topic, key.orElse(null), payload);

        CompletableFuture<Void> future = new CompletableFuture<>();

        producer.send(
                record,
                (metadata, exception) -> {
                    if (exception != null) {
                        future.completeExceptionally(exception);
                    } else {
                        future.complete(null);
                    }
                });

        return future;
    }

    @Override
    public void close() throws Exception {
        producer.close();
    }

    private static byte[] toBytes(Object[] payload) {
        int totalSize = 0;
        for (Object obj : payload) {
            if (obj instanceof Integer) {
                totalSize += Integer.BYTES;
            } else if (obj instanceof String) {
                totalSize += Integer.BYTES + ((String) obj).getBytes(StandardCharsets.UTF_8).length;
            } else if (obj instanceof Long) {
                totalSize += Long.BYTES;
            } else {
                throw new IllegalArgumentException("Unsupported type: " + obj.getClass().getName());
            }
        }

        ByteBuffer buffer = ByteBuffer.allocate(totalSize);

        for (Object obj : payload) {
            if (obj instanceof Integer) {
                buffer.putInt((Integer) obj);
            } else if (obj instanceof String) {
                byte[] stringBytes = ((String) obj).getBytes(StandardCharsets.UTF_8);
                buffer.putInt(stringBytes.length);
                buffer.put(stringBytes);
            } else if (obj instanceof Long) {
                buffer.putLong((Long) obj);
            }
        }

        return buffer.array();
    }
}
