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

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.runtime.security.SecurityConfiguration;
import org.apache.flink.runtime.util.HadoopUtils;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.security.token.TokenIdentifier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.security.auth.Subject;

import java.io.File;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Collection;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * HadoopModule包含了Flink的SecurityConfiguration和Hadoop的配置信息
 * Responsible for installing a Hadoop login user.
 * */
public class HadoopModule implements SecurityModule {

    private static final Logger LOG = LoggerFactory.getLogger(HadoopModule.class);

    private final SecurityConfiguration securityConfig;

    private final Configuration hadoopConfiguration;

    public HadoopModule(
            SecurityConfiguration securityConfiguration, Configuration hadoopConfiguration) {
        this.securityConfig = checkNotNull(securityConfiguration);
        this.hadoopConfiguration = checkNotNull(hadoopConfiguration);
    }

    @VisibleForTesting
    public SecurityConfiguration getSecurityConfig() {
        return securityConfig;
    }


    // install方法使用Hadoop提供的UserGroupInformation进行认证操作。
    @Override
    public void install() throws SecurityInstallException {



        // UGI设置hadoop conf
        UserGroupInformation.setConfiguration(hadoopConfiguration);

        UserGroupInformation loginUser;

        try {
            // 如果Hadoop启用了安全配置
            if (UserGroupInformation.isSecurityEnabled()
                    && !StringUtils.isBlank(securityConfig.getKeytab())
                    && !StringUtils.isBlank(securityConfig.getPrincipal())) {

                // 获取keytab路径
                String keytabPath = (new File(securityConfig.getKeytab())).getAbsolutePath();

                // 使用UGI认证Flink conf中配置的keytab和principal
                UserGroupInformation.loginUserFromKeytab(securityConfig.getPrincipal(), keytabPath);

                // 获取认证的用户
                loginUser = UserGroupInformation.getLoginUser();

                // 从HADOOP_TOKEN_FILE_LOCATION读取token缓存文件
                // supplement with any available tokens
                String fileLocation =
                        System.getenv(UserGroupInformation.HADOOP_TOKEN_FILE_LOCATION);

                // 如果有本地token缓存
                if (fileLocation != null) {
                    Credentials credentialsFromTokenStorageFile =
                            Credentials.readTokenStorageFile(
                                    new File(fileLocation), hadoopConfiguration);

                    // if UGI uses Kerberos keytabs for login, do not load HDFS delegation token
                    // since
                    // the UGI would prefer the delegation token instead, which eventually expires
                    // and does not fallback to using Kerberos tickets

                    // 如果UGI使用keytab方式登录，不用加载HDFS的delegation token
                    // 因为UGI倾向于使用delegation token，这些token最终会失效，不会使用kerberos票据
                    Credentials credentialsToBeAdded = new Credentials();
                    final Text hdfsDelegationTokenKind = new Text("HDFS_DELEGATION_TOKEN");
                    Collection<Token<? extends TokenIdentifier>> usrTok =
                            credentialsFromTokenStorageFile.getAllTokens();
                    // If UGI use keytab for login, do not load HDFS delegation token.

                    // 遍历token存储文件中的token
                    // 将所有的非delegation token添加到凭据中
                    for (Token<? extends TokenIdentifier> token : usrTok) {
                        if (!token.getKind().equals(hdfsDelegationTokenKind)) {
                            final Text id = new Text(token.getIdentifier());
                            credentialsToBeAdded.addToken(id, token);
                        }
                    }

                    // 为loginUser添加凭据
                    loginUser.addCredentials(credentialsToBeAdded);
                }
            } else {

                // 如果没有启动安全配置
                // 从当前用户凭据认证

                // login with current user credentials (e.g. ticket cache, OS login)
                // note that the stored tokens are read automatically

                // 反射调用如下方法
                // UserGroupInformation.loginUserFromSubject(null);
                try {
                    // Use reflection API to get the login user object
                    // UserGroupInformation.loginUserFromSubject(null);
                    Method loginUserFromSubjectMethod =
                            UserGroupInformation.class.getMethod(
                                    "loginUserFromSubject", Subject.class);
                    loginUserFromSubjectMethod.invoke(null, (Subject) null);
                } catch (NoSuchMethodException e) {
                    LOG.warn("Could not find method implementations in the shaded jar.", e);
                } catch (InvocationTargetException e) {
                    throw e.getTargetException();
                }
                // 获取当前登录用户
                loginUser = UserGroupInformation.getLoginUser();
            }

            LOG.info("Hadoop user set to {}", loginUser);

            if (HadoopUtils.isKerberosSecurityEnabled(loginUser)) {
                boolean isCredentialsConfigured =
                        HadoopUtils.areKerberosCredentialsValid(
                                loginUser, securityConfig.useTicketCache());

                LOG.info(
                        "Kerberos security is enabled and credentials are {}.",
                        isCredentialsConfigured ? "valid" : "invalid");
            }
        } catch (Throwable ex) {
            throw new SecurityInstallException("Unable to set the Hadoop login user", ex);
        }
    }

    @Override
    public void uninstall() {
        throw new UnsupportedOperationException();
    }
}
