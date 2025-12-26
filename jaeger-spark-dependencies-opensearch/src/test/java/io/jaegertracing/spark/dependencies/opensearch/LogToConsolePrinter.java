/**
 * Copyright 2017 The Jaeger Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */
package io.jaegertracing.spark.dependencies.opensearch;

import java.util.function.Consumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.output.OutputFrame;

public class LogToConsolePrinter implements Consumer<OutputFrame> {
    private static final Logger log = LoggerFactory.getLogger(LogToConsolePrinter.class);
    private final String prefix;

    public LogToConsolePrinter(String prefix) {
        this.prefix = prefix;
    }

    @Override
    public void accept(OutputFrame outputFrame) {
        String utf8String = outputFrame.getUtf8String();
        if (utf8String != null) {
            System.out.print(prefix + utf8String);
        }
    }
}
