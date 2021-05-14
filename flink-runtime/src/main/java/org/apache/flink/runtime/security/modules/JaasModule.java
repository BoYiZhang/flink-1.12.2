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

package org.apache.flink.runtime.security.modules;

import org.apache.flink.annotation.Internal;
import org.apache.flink.configuration.CoreOptions;
import org.apache.flink.runtime.security.DynamicConfiguration;
import org.apache.flink.runtime.security.KerberosUtils;
import org.apache.flink.runtime.security.SecurityConfiguration;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.security.auth.login.AppConfigurationEntry;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;

import static org.apache.flink.configuration.ConfigurationUtils.splitPaths;
import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.Preconditions.checkState;

/**
 * Responsible for installing a process-wide JAAS configuration.
 *
 * <p>The installed configuration combines login modules based on: - the user-supplied JAAS
 * configuration file, if any - a Kerberos keytab, if configured - any cached Kerberos credentials
 * from the current environment
 *
 * <p>The module also installs a default JAAS config file (if necessary) for compatibility with ZK
 * and Kafka. Note that the JRE actually draws on numerous file locations. See:
 * https://docs.oracle.com/javase/7/docs/jre/api/security/jaas/spec/com/sun/security/auth/login/ConfigFile.html
 * See:
 * https://github.com/apache/kafka/blob/0.9.0/clients/src/main/java/org/apache/kafka/common/security/kerberos/Login.java#L289
 */
@Internal
public class JaasModule implements SecurityModule {

    private static final Logger LOG = LoggerFactory.getLogger(JaasModule.class);

    static final String JAVA_SECURITY_AUTH_LOGIN_CONFIG = "java.security.auth.login.config";

    static final String JAAS_CONF_RESOURCE_NAME = "flink-jaas.conf";

    private final SecurityConfiguration securityConfig;

    private String priorConfigFile;
    private javax.security.auth.login.Configuration priorConfig;

    private DynamicConfiguration currentConfig;

    /** The working directory that the jaas file will install into. */
    private final String workingDir;

    public JaasModule(SecurityConfiguration securityConfig) {
        this.securityConfig = checkNotNull(securityConfig);
        String[] dirs = splitPaths(securityConfig.getFlinkConfig().getString(CoreOptions.TMP_DIRS));
        // should be at least one directory.
        checkState(dirs.length > 0);
        this.workingDir = dirs[0];
    }

    // install方法读取了java.security.auth.login.config系统变量对应的jaas配置，
    // 并且将Flink配置文件中相关配置转换为JAAS中的entry，
    // 合并到系统变量对应的jaas配置中并设置给JVM。
    @Override
    public void install() {

        // ensure that a config file is always defined, for compatibility with
        // ZK and Kafka which check for the system property and existence of the file

        // 读取java.security.auth.login.config系统变量值，用于在卸载module的时候恢复

        priorConfigFile = System.getProperty(JAVA_SECURITY_AUTH_LOGIN_CONFIG, null);

        // 如果没有配置
        if (priorConfigFile == null) {
            // Flink的 io.tmp.dirs配置项第一个目录为workingDir
            // 将默认的flink-jaas.conf文件写入这个位置，创建临时文件，名为jass-xxx.conf
            // 在JVM进程关闭的时候删除这个临时文件
            File configFile = generateDefaultConfigFile(workingDir);
            // 配置java.security.auth.login.config系统变量值
            // 保证这个系统变量的值始终存在，这是为了兼容Zookeeper和Kafka
            // 他们会去检查这个jaas文件是否存在
            System.setProperty(JAVA_SECURITY_AUTH_LOGIN_CONFIG, configFile.getAbsolutePath());
            LOG.info("Jaas file will be created as {}.", configFile);
        }

        // read the JAAS configuration file
        // 读取已安装的jaas配置文件
        priorConfig = javax.security.auth.login.Configuration.getConfiguration();

        // construct a dynamic JAAS configuration
        // 包装为DynamicConfiguration，这个配置是可以修改的
        currentConfig = new DynamicConfiguration(priorConfig);

        // wire up the configured JAAS login contexts to use the krb5 entries
        // 从Flink配置文件中读取kerberos配置
        // AppConfigurationEntry为Java读取Jaas配置文件中一段配置项的封装
        // 一段配置项指的是大括号之内的配置
        AppConfigurationEntry[] krb5Entries = getAppConfigurationEntries(securityConfig);
        if (krb5Entries != null) {
            // 遍历Flink配置项security.kerberos.login.contexts，作为entry name使用
            for (String app : securityConfig.getLoginContextNames()) {
                // 将krb5Entries对应的AppConfigurationEntry添加入currrentConfig
                // 使用security.kerberos.login.contexts对应的entry name
                currentConfig.addAppConfigurationEntry(app, krb5Entries);
            }
        }
        // 设置新的currentConfig
        javax.security.auth.login.Configuration.setConfiguration(currentConfig);
    }

    @Override
    public void uninstall() throws SecurityInstallException {
        if (priorConfigFile != null) {
            System.setProperty(JAVA_SECURITY_AUTH_LOGIN_CONFIG, priorConfigFile);
        } else {
            System.clearProperty(JAVA_SECURITY_AUTH_LOGIN_CONFIG);
        }
        javax.security.auth.login.Configuration.setConfiguration(priorConfig);
    }

    public DynamicConfiguration getCurrentConfiguration() {
        return currentConfig;
    }

    /**
     *     getAppConfigurationEntries方法从Flink的securityConfig中读取配置，
     *     转换为JAAS entry的格式，存入AppConfigurationEntry。
     *     如果Flink配置了security.kerberos.login.use-ticket-cache，
     *     加载类似如下内容的文件，生成一个AppConfigurationEntry叫做userKerberosAce
     *
     *      {@code
     *         EntryName {
     *             com.sun.security.auth.module.Krb5LoginModule optional
     *             doNotPrompt=true
     *             useTicketCache=true
     *             renewTGT=true;
     *         };
     *       }
     *
     *  如果Flink中配置了security.kerberos.login.keytab，会加载如下配置，
     *  生成一个AppConfigurationEntry叫做keytabKerberosAce：
     *
     *  {@code
     *       EntryName {
     *           com.sun.security.auth.module.Krb5LoginModule required
     *           keyTab=keytab路径
     *           doNotPrompt=true
     *           useKeyTab=true
     *           storeKey=true
     *           principal=principal名称
     *           refreshKrb5Config=true;
     *       };
     *
     *
     *  }
     *
     *
     *  getAppConfigurationEntries最后返回这两个AppConfigurationEntry的集合，
     *  如果某一个为null，只返回其中一个。
     *
     * @param securityConfig
     * @return
     */


    private static AppConfigurationEntry[] getAppConfigurationEntries(
            SecurityConfiguration securityConfig) {

        AppConfigurationEntry userKerberosAce = null;
        if (securityConfig.useTicketCache()) {
            userKerberosAce = KerberosUtils.ticketCacheEntry();
        }
        AppConfigurationEntry keytabKerberosAce = null;
        if (securityConfig.getKeytab() != null) {
            keytabKerberosAce =
                    KerberosUtils.keytabEntry(
                            securityConfig.getKeytab(), securityConfig.getPrincipal());
        }

        AppConfigurationEntry[] appConfigurationEntry;
        if (userKerberosAce != null && keytabKerberosAce != null) {
            appConfigurationEntry =
                    new AppConfigurationEntry[] {keytabKerberosAce, userKerberosAce};
        } else if (keytabKerberosAce != null) {
            appConfigurationEntry = new AppConfigurationEntry[] {keytabKerberosAce};
        } else if (userKerberosAce != null) {
            appConfigurationEntry = new AppConfigurationEntry[] {userKerberosAce};
        } else {
            return null;
        }

        return appConfigurationEntry;
    }

    /** Generate the default JAAS config file. */
    private static File generateDefaultConfigFile(String workingDir) {
        checkArgument(workingDir != null, "working directory should not be null.");
        final File jaasConfFile;
        try {
            Path path = Paths.get(workingDir);
            if (Files.notExists(path)) {
                // We intentionally favored Path.toRealPath over Files.readSymbolicLinks as the
                // latter one might return a
                // relative path if the symbolic link refers to it. Path.toRealPath resolves the
                // relative path instead.
                Path parent = path.getParent().toRealPath();
                Path resolvedPath = Paths.get(parent.toString(), path.getFileName().toString());

                path = Files.createDirectories(resolvedPath);
            }
            Path jaasConfPath = Files.createTempFile(path, "jaas-", ".conf");
            try (InputStream resourceStream =
                    JaasModule.class
                            .getClassLoader()
                            .getResourceAsStream(JAAS_CONF_RESOURCE_NAME)) {
                Files.copy(resourceStream, jaasConfPath, StandardCopyOption.REPLACE_EXISTING);
            }
            jaasConfFile = new File(workingDir, jaasConfPath.getFileName().toString());
            jaasConfFile.deleteOnExit();
        } catch (IOException e) {
            throw new RuntimeException("unable to generate a JAAS configuration file", e);
        }
        return jaasConfFile;
    }
}
