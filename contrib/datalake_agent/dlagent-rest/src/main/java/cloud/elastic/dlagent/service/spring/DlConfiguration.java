/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * 
 *   http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package cloud.elastic.dlagent.service.spring;

import cloud.elastic.dlagent.api.configuration.DlServerProperties;
import cloud.elastic.dlagent.api.configuration.GopherConfigurationProperties;
import cloud.elastic.dlagent.api.configuration.IcebergRestProperties;
import cloud.elastic.dlagent.api.configuration.TaskProperties;
import cloud.elastic.dlagent.transport.JsonResponseWriter;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.ListableBeanFactory;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.task.TaskExecutorBuilder;
import org.springframework.boot.task.TaskExecutorCustomizer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.task.AsyncTaskExecutor;
import org.springframework.core.task.TaskDecorator;
import org.springframework.core.task.TaskExecutor;
import org.springframework.scheduling.annotation.AsyncAnnotationBeanPostProcessor;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;
import org.springframework.web.servlet.config.annotation.AsyncSupportConfigurer;
import org.springframework.web.servlet.config.annotation.WebMvcConfigurer;

/**
 * Configures the {@link AsyncTaskExecutor} for tasks that will stream data to
 * clients
 */
@Configuration
public class DlConfiguration implements WebMvcConfigurer {

    /**
     * Bean name of dlagent's {@link TaskExecutor}.
     */
    public static final String DLAGENT_RESPONSE_STREAM_TASK_EXECUTOR = "dlagentResponseStreamTaskExecutor";
    private static final Logger LOG = LoggerFactory.getLogger(DlConfiguration.class);

    private final ListableBeanFactory beanFactory;

    /**
     * Constructs a dlagent Configuration object with the provided
     * {@link ListableBeanFactory}
     *
     * @param beanFactory the beanFactory
     */
    public DlConfiguration(ListableBeanFactory beanFactory) {
        this.beanFactory = beanFactory;
    }

    /**
     * Binds {@code dlagent.*} onto the plain POJO that lives in dlagent-core.
     * <p>
     * dlagent-core cannot carry {@code @ConfigurationProperties} itself (that
     * is a spring-boot annotation), so the binding is declared here instead of
     * with {@code @EnableConfigurationProperties(DlServerProperties.class)}.
     * Same class, same prefix, therefore same bound values.
     *
     * @return the bound dlagent properties
     */
    @Bean
    @ConfigurationProperties(prefix = DlServerProperties.PROPERTY_PREFIX)
    public DlServerProperties dlServerProperties() {
        return new DlServerProperties();
    }

    /**
     * Binds {@code gopher.*} onto the plain POJO that lives in dlagent-core;
     * same reasoning as {@link #dlServerProperties()}. It used to be a
     * component-scanned {@code @Component @ConfigurationProperties} class.
     *
     * @return the bound gopher properties
     */
    /**
     * Makes Spring MVC serialize responses with the SAME ObjectMapper INSTANCE
     * that dlagent-grpc will use, rather than with a separately configured one.
     *
     * <p>This is how "both transports hand the C side identical bytes" is
     * enforced on the REST side. Defining this bean makes Boot's
     * JacksonAutoConfiguration back off
     * ({@code @ConditionalOnMissingBean}) and Spring's
     * MappingJackson2HttpMessageConverter pick this one up, so there is one
     * mapper in the process instead of two that have to be kept in step by
     * hand -- the failure mode being a Boot upgrade that shifts one Jackson
     * default and silently splits the two transports on dates, nulls or field
     * order, which no semantic test can see.
     *
     * <p>The instance is the one {@link JsonResponseWriter} owns, so REST and
     * gRPC cannot diverge even in principle. It is configured to match what
     * Boot would have auto-configured, measured rather than assumed; the
     * remaining difference is the two Boot-internal modules proven to be
     * no-ops in this repository. {@code BootMapperParityTest} re-checks both
     * halves of that on every build, and
     * {@code SpringConverterByteEquivalenceTest} checks that Spring's
     * converter and the writer emit the same bytes through it.
     */
    /**
     * Binds the {@code iceberg.rest} prefix, exactly as the old
     * {@code service.spring.IcebergRestConfig} did.
     *
     * <p>The POJO lives in dlagent-core because {@code planFileGroups}, a core
     * operation, reads two of its values, and core may not reference
     * {@code @ConfigurationProperties}. Same class fields, same
     * prefix, therefore same resolved values, so the REST path's behaviour
     * does not change.
     */
    @Bean
    @ConfigurationProperties(prefix = IcebergRestProperties.PROPERTY_PREFIX)
    public IcebergRestProperties icebergRestProperties() {
        return new IcebergRestProperties();
    }

    @Bean
    public ObjectMapper objectMapper(JsonResponseWriter jsonResponseWriter) {
        return jsonResponseWriter.objectMapper();
    }

    @Bean
    @ConfigurationProperties(prefix = GopherConfigurationProperties.PROPERTY_PREFIX)
    public GopherConfigurationProperties gopherConfigurationProperties() {
        return new GopherConfigurationProperties();
    }

    /**
     * Configures the TaskExecutor to be used for async requests (i.e. Bridge
     * Read).
     */
    @Override
    public void configureAsyncSupport(AsyncSupportConfigurer configurer) {
        AsyncTaskExecutor taskExecutor = (AsyncTaskExecutor) this.beanFactory
                .getBean(DLAGENT_RESPONSE_STREAM_TASK_EXECUTOR);
        configurer.setTaskExecutor(taskExecutor);
    }

    /**
     * Configures and builds the {@link ThreadPoolTaskExecutor}
     *
     * @return the {@link ThreadPoolTaskExecutor}
     */
    @Bean(name = {DLAGENT_RESPONSE_STREAM_TASK_EXECUTOR,
            AsyncAnnotationBeanPostProcessor.DEFAULT_TASK_EXECUTOR_BEAN_NAME})
    public ThreadPoolTaskExecutor dlagentApplicationTaskExecutor(DlServerProperties dlagentServerProperties,
                                                                 ObjectProvider<TaskExecutorCustomizer> taskExecutorCustomizers,
                                                                 ObjectProvider<TaskDecorator> taskDecorator) {

        TaskProperties properties = dlagentServerProperties.getTask();
        TaskProperties.Pool pool = properties.getPool();
        TaskExecutorBuilder builder = new TaskExecutorBuilder();
        builder = builder.queueCapacity(pool.getQueueCapacity());
        builder = builder.corePoolSize(pool.getCoreSize());
        builder = builder.maxPoolSize(pool.getMaxSize());
        builder = builder.allowCoreThreadTimeOut(pool.isAllowCoreThreadTimeout());
        builder = builder.keepAlive(pool.getKeepAlive());
        TaskProperties.Shutdown shutdown = properties.getShutdown();
        builder = builder.awaitTermination(shutdown.isAwaitTermination());
        builder = builder.awaitTerminationPeriod(shutdown.getAwaitTerminationPeriod());
        builder = builder.threadNamePrefix(properties.getThreadNamePrefix());
        builder = builder.customizers(taskExecutorCustomizers.orderedStream()::iterator);
        builder = builder.taskDecorator(taskDecorator.getIfUnique());

        LOG.debug("Initializing dlagent ThreadPoolTaskExecutor with prefix={}. " +
                        "Pool options: " +
                        "queue capacity={}, core size={}, max size={}, " +
                        "allow core thread timeout={}, keep alive={}. " +
                        "Shutdown options: await termination={}, await " +
                        "termination period={}.",
                properties.getThreadNamePrefix(),
                pool.getQueueCapacity(),
                pool.getCoreSize(),
                pool.getMaxSize(),
                pool.isAllowCoreThreadTimeout(),
                pool.getKeepAlive(),
                shutdown.isAwaitTermination(),
                shutdown.getAwaitTerminationPeriod());

        return builder.build(DlThreadPoolTaskExecutor.class);
    }

}
