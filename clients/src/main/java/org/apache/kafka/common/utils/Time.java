/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.kafka.common.utils;

/**
 * An interface abstracting the clock to use in unit testing classes that make use of clock time
 */
public interface Time {

    public static long NS_PER_US = 1000;
    public static long US_PER_MS = 1000;
    public static long MS_PER_SEC = 1000;
    public static long NS_PER_MS = NS_PER_US * US_PER_MS;
    public static long NS_PER_SEC = NS_PER_MS * MS_PER_SEC;
    public static long US_PER_SEC = US_PER_MS * MS_PER_SEC;
    public static long SECS_PER_MIN = 60;
    public static long MINS_PER_HOUR = 60;
    public static long HOURS_PER_DAY = 24;
    public static long SECS_PER_HOUR = SECS_PER_MIN * MINS_PER_HOUR;
    public static long SECS_PER_DAY = SECS_PER_HOUR * HOURS_PER_DAY;
    public static long MINS_PER_DAY = MINS_PER_HOUR * HOURS_PER_DAY;
    
    /**
     * The current time in milliseconds
     */
    public long milliseconds();

    /**
     * The current time in nanoseconds
     */
    public long nanoseconds();

    /**
     * Sleep for the given number of milliseconds
     */
    public void sleep(long ms);

}
