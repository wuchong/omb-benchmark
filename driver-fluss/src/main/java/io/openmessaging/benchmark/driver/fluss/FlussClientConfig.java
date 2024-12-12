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

/** Fluss client config. */
public class FlussClientConfig {
    public String bootstrapServers;

    public String writerAcks;

    public String writerBatchSize;

    public String writerBufferMemory;

    public int writerBatchTimeoutMs;

    public int clientNettyThreads;

    public String fetchMaxBytes;

    public String logFormat;

    public int prefetchNum;

    public String projectFields;

    public boolean isCheckCrc;

    public String pageSize;

    public int arrowRecordNum;
}
