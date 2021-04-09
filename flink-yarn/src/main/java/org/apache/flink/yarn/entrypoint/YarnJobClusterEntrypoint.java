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

package org.apache.flink.yarn.entrypoint;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.entrypoint.ClusterEntrypoint;
import org.apache.flink.runtime.entrypoint.ClusterEntrypointUtils;
import org.apache.flink.runtime.entrypoint.DynamicParametersConfigurationParserFactory;
import org.apache.flink.runtime.entrypoint.JobClusterEntrypoint;
import org.apache.flink.runtime.entrypoint.component.DefaultDispatcherResourceManagerComponentFactory;
import org.apache.flink.runtime.entrypoint.component.FileJobGraphRetriever;
import org.apache.flink.runtime.util.EnvironmentInformation;
import org.apache.flink.runtime.util.JvmShutdownSafeguard;
import org.apache.flink.runtime.util.SignalHandler;
import org.apache.flink.util.Preconditions;
import org.apache.flink.yarn.configuration.YarnConfigOptions;

import org.apache.hadoop.yarn.api.ApplicationConstants;

import java.io.IOException;
import java.util.Map;

/** Entry point for Yarn per-job clusters. */
public class YarnJobClusterEntrypoint extends JobClusterEntrypoint {

    public YarnJobClusterEntrypoint(Configuration configuration) {
        super(configuration);
    }

    @Override
    protected String getRPCPortRange(Configuration configuration) {
        return configuration.getString(YarnConfigOptions.APPLICATION_MASTER_PORT);
    }

    @Override
    protected DefaultDispatcherResourceManagerComponentFactory
            createDispatcherResourceManagerComponentFactory(Configuration configuration)
                    throws IOException {

        return DefaultDispatcherResourceManagerComponentFactory.createJobComponentFactory(
                YarnResourceManagerFactory.getInstance(),
                FileJobGraphRetriever.createFrom(
                        configuration,
                        YarnEntrypointUtils.getUsrLibDir(configuration).orElse(null)));
    }

    // ------------------------------------------------------------------------
    //  The executable entry point for the Yarn Application Master Process
    //  for a single Flink job.
    // ------------------------------------------------------------------------

    public static void main(String[] args) {
        // startup checks and logging
        EnvironmentInformation.logEnvironmentInfo(
                LOG, YarnJobClusterEntrypoint.class.getSimpleName(), args);
        SignalHandler.register(LOG);
        JvmShutdownSafeguard.installAsShutdownHook(LOG);


        //    env = {Collections$UnmodifiableMap@2653}  size = 40
        //    "PATH" -> "/usr/local/bin:/usr/bin"
        //    "HADOOP_CONF_DIR" -> "/opt/tools/hadoop-3.1.3/etc/hadoop"
        //    "JAVA_HOME" -> "/opt/java/jdk1.8"
        //    "XFILESEARCHPATH" -> "/usr/dt/app-defaults/%L/Dt"
        //    "_CLIENT_HOME_DIR" -> "hdfs://henghe-030:8020/user/yarn"
        //    "LANG" -> "zh_CN.UTF-8"
        //    "APP_SUBMIT_TIME_ENV" -> "1617185635780"
        //    "NM_HOST" -> "henghe-030"
        //    "_APP_ID" -> "application_1617174387689_0008"
        //    "HADOOP_USER_NAME" -> "yarn/henghe-030@HENGHE.COM"
        //    "HADOOP_HDFS_HOME" -> "/opt/tools/hadoop-3.1.3"
        //    "LOGNAME" -> "yarn"
        //    "JVM_PID" -> "25006"
        //    "PWD" -> "/opt/tools/hadoop-3.1.3/data/local-dirs/usercache/yarn/appcache/application_1617174387689_0008/container_1617174387689_0008_01_000001"
        //    "HADOOP_COMMON_HOME" -> "/opt/tools/hadoop-3.1.3"
        //    "_" -> "/opt/java/jdk1.8/bin/java"
        //    "LOCAL_DIRS" -> "/opt/tools/hadoop-3.1.3/data/local-dirs/usercache/yarn/appcache/application_1617174387689_0008"
        //    "APPLICATION_WEB_PROXY_BASE" -> "/proxy/application_1617174387689_0008"
        //    "NM_HTTP_PORT" -> "8042"
        //    "LOG_DIRS" -> "/opt/tools/hadoop-3.1.3/data/logs/userlogs/application_1617174387689_0008/container_1617174387689_0008_01_000001"
        //    "PRELAUNCH_OUT" -> "/opt/tools/hadoop-3.1.3/data/logs/userlogs/application_1617174387689_0008/container_1617174387689_0008_01_000001/prelaunch.out"
        //    "NM_AUX_SERVICE_mapreduce_shuffle" -> "AAA0+gAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA="
        //    "NM_PORT" -> "40217"
        //    "_CLIENT_SHIP_FILES" -> "YarnLocalResourceDescriptor{key=lib/log4j-slf4j-impl-2.12.1.jar, path=hdfs://henghe-030:8020/user/yarn/.flink/application_1617174387689_0008/lib/log4j-slf4j-impl-2.12.1.jar, size=23518, modificationTime=1617185610971, visibility=APPLICATION, type=FILE};YarnLocalResourceDescriptor{key=lib/log4j-core-2.12.1.jar, path=hdfs://henghe-030:8020/user/yarn/.flink/application_1617174387689_0008/lib/log4j-core-2.12.1.jar, size=1674433, modificationTime=1617185611190, visibility=APPLICATION, type=FILE};YarnLocalResourceDescriptor{key=lib/flink-shaded-zookeeper-3.4.14.jar, path=hdfs://henghe-030:8020/user/yarn/.flink/application_1617174387689_0008/lib/flink-shaded-zookeeper-3.4.14.jar, size=7709741, modificationTime=1617185611931, visibility=APPLICATION, type=FILE};YarnLocalResourceDescriptor{key=lib/log4j-1.2-api-2.12.1.jar, path=hdfs://henghe-030:8020/user/yarn/.flink/application_1617174387689_0008/lib/log4j-1.2-api-2.12.1.jar, size=67114, modificationTime=1617185611978, visibility=APPLICATION, t"
        //    "HADOOP_YARN_HOME" -> "/opt/tools/hadoop-3.1.3"
        //    "USER" -> "yarn"
        //    "CLASSPATH" -> ":SocketWindowWordCount.jar:lib/flink-csv-1.12.0.jar:lib/flink-json-1.12.0.jar:lib/flink-shaded-hadoop-3-uber-3.1.1.7.1.1.0-565-9.0.jar:lib/flink-shaded-zookeeper-3.4.14.jar:lib/flink-table-blink_2.11-1.12.0.jar:lib/flink-table_2.11-1.12.0.jar:lib/hk2-api-2.5.0-b32.jar:lib/hk2-locator-2.5.0-b32.jar:lib/hk2-utils-2.5.0-b32.jar:lib/javax.inject-2.5.0-b32.jar:lib/javax.ws.rs-api-2.0.jar:lib/jersey-common-2.25.1.jar:lib/jersey-core-1.19.jar:lib/jersey-guava-2.25.1.jar:lib/log4j-1.2-api-2.12.1.jar:lib/log4j-api-2.12.1.jar:lib/log4j-core-2.12.1.jar:lib/log4j-slf4j-impl-2.12.1.jar:flink-dist_2.11-1.12.0.jar:job.graph:flink-conf.yaml::/opt/tools/hadoop-3.1.3/etc/hadoop:/opt/tools/hadoop-3.1.3/share/hadoop/common/*:/opt/tools/hadoop-3.1.3/share/hadoop/common/lib/*:/opt/tools/hadoop-3.1.3/share/hadoop/hdfs/*:/opt/tools/hadoop-3.1.3/share/hadoop/hdfs/lib/*:/opt/tools/hadoop-3.1.3/share/hadoop/yarn/*:/opt/tools/hadoop-3.1.3/share/hadoop/yarn/lib/*"
        //    "PRELAUNCH_ERR" -> "/opt/tools/hadoop-3.1.3/data/logs/userlogs/application_1617174387689_0008/container_1617174387689_0008_01_000001/prelaunch.err"
        //    "_FLINK_YARN_FILES" -> "hdfs://henghe-030:8020/user/yarn/.flink/application_1617174387689_0008"
        //    "_ZOOKEEPER_NAMESPACE" -> "application_1617174387689_0008"
        //    "HADOOP_TOKEN_FILE_LOCATION" -> "/opt/tools/hadoop-3.1.3/data/local-dirs/usercache/yarn/appcache/application_1617174387689_0008/container_1617174387689_0008_01_000001/container_tokens"
        //    "NLSPATH" -> "/usr/dt/lib/nls/msg/%L/%N.cat"
        //    "LOCAL_USER_DIRS" -> "/opt/tools/hadoop-3.1.3/data/local-dirs/usercache/yarn/"
        //    "HADOOP_HOME" -> "/opt/tools/hadoop-3.1.3"
        //    "_FLINK_CLASSPATH" -> ":SocketWindowWordCount.jar:lib/flink-csv-1.12.0.jar:lib/flink-json-1.12.0.jar:lib/flink-shaded-hadoop-3-uber-3.1.1.7.1.1.0-565-9.0.jar:lib/flink-shaded-zookeeper-3.4.14.jar:lib/flink-table-blink_2.11-1.12.0.jar:lib/flink-table_2.11-1.12.0.jar:lib/hk2-api-2.5.0-b32.jar:lib/hk2-locator-2.5.0-b32.jar:lib/hk2-utils-2.5.0-b32.jar:lib/javax.inject-2.5.0-b32.jar:lib/javax.ws.rs-api-2.0.jar:lib/jersey-common-2.25.1.jar:lib/jersey-core-1.19.jar:lib/jersey-guava-2.25.1.jar:lib/log4j-1.2-api-2.12.1.jar:lib/log4j-api-2.12.1.jar:lib/log4j-core-2.12.1.jar:lib/log4j-slf4j-impl-2.12.1.jar:flink-dist_2.11-1.12.0.jar:job.graph:flink-conf.yaml:"
        //    "_FLINK_DIST_JAR" -> "YarnLocalResourceDescriptor{key=flink-dist_2.11-1.12.0.jar, path=hdfs://henghe-030:8020/user/yarn/.flink/application_1617174387689_0008/flink-dist_2.11-1.12.0.jar, size=120232069, modificationTime=1617185635268, visibility=APPLICATION, type=FILE}"
        //    "HOME" -> "/home/"
        //    "SHLVL" -> "1"
        //    "CONTAINER_ID" -> "container_1617174387689_0008_01_000001"
        //    "MALLOC_ARENA_MAX" -> "4"
        Map<String, String> env = System.getenv();

        // /opt/tools/hadoop-3.1.3/data/local-dirs/usercache/yarn/appcache/application_1617174387689_0008/container_1617174387689_0008_01_000001
        final String workingDirectory = env.get(ApplicationConstants.Environment.PWD.key());

        Preconditions.checkArgument(
                workingDirectory != null,
                "Working directory variable (%s) not set",
                ApplicationConstants.Environment.PWD.key());

        try {
            YarnEntrypointUtils.logYarnEnvironmentInformation(env, LOG);
        } catch (IOException e) {
            LOG.warn("Could not log YARN environment information.", e);
        }

        //  {
        //      jobmanager.memory.off-heap.size=134217728b,
        //      jobmanager.memory.jvm-overhead.min=201326592b,
        //      jobmanager.memory.jvm-metaspace.size=268435456b,
        //      jobmanager.memory.heap.size=1073741824b,
        //      jobmanager.memory.jvm-overhead.max=201326592b
        //  }
        final Configuration dynamicParameters =
                ClusterEntrypointUtils.parseParametersOrExit(
                        args,
                        new DynamicParametersConfigurationParserFactory(),
                        YarnJobClusterEntrypoint.class);
        //        {
        //            internal.jobgraph-path=job.graph,
        //            env.java.opts.jobmanager=-agentlib:jdwp=transport=dt_socket,server=y,suspend=y,address=5006,
        //            jobmanager.execution.failover-strategy=region,
        //            high-availability.cluster-id=application_1617174387689_0008,
        //            jobmanager.rpc.address=henghe-030,
        //            jobmanager.memory.jvm-overhead.min=201326592b,
        //            execution.savepoint.ignore-unclaimed-state=false,
        //            io.tmp.dirs=/opt/tools/hadoop-3.1.3/data/local-dirs/usercache/yarn/appcache/application_1617174387689_0008,
        //            parallelism.default=1,
        //            taskmanager.numberOfTaskSlots=1,
        //            taskmanager.memory.process.size=1728m, web.port=0,
        //            jobmanager.memory.off-heap.size=134217728b,
        //            execution.target=yarn-per-job,
        //            jobmanager.memory.process.size=1600m,
        //            jobmanager.rpc.port=6123,
        //            internal.io.tmpdirs.use-local-default=true,
        //            execution.attached=true,
        //            internal.cluster.execution-mode=NORMAL,
        //            execution.shutdown-on-attached-exit=false,
        //            pipeline.jars=file:/opt/tools/flink-1.12.0/examples/streaming/SocketWindowWordCount.jar,
        //            rest.address=henghe-030,
        //            jobmanager.memory.jvm-metaspace.size=268435456b,
        //            $internal.deployment.config-dir=/opt/tools/flink-1.12.0/conf,
        //            $internal.yarn.log-config-file=/opt/tools/flink-1.12.0/conf/log4j.properties,
        //            jobmanager.memory.heap.size=1073741824b,
        //            jobmanager.memory.jvm-overhead.max=201326592b
        //      }
        // 1.构建配置
        final Configuration configuration =
                YarnEntrypointUtils.loadConfiguration(workingDirectory, dynamicParameters, env);

        // 2.构建YarnJobClusterEntrypoint
        YarnJobClusterEntrypoint yarnJobClusterEntrypoint = new YarnJobClusterEntrypoint(configuration);

        // 3.启动
        ClusterEntrypoint.runClusterEntrypoint(yarnJobClusterEntrypoint);
    }
}
