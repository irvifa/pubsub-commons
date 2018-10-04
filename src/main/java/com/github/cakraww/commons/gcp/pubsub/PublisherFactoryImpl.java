package com.github.cakraww.commons.gcp.pubsub;

import com.github.cakraww.commons.gcp.pubsub.core.ExecutorProviderKind;
import com.github.cakraww.commons.gcp.pubsub.core.PerPublisherExecutor;
import com.github.cakraww.commons.gcp.pubsub.core.SharedExecutor;
import com.google.api.gax.batching.BatchingSettings;
import com.google.api.gax.core.CredentialsProvider;
import com.google.api.gax.core.FixedCredentialsProvider;
import com.google.api.gax.grpc.GrpcStatusCode;
import com.google.api.gax.retrying.RetrySettings;
import com.google.api.gax.rpc.ApiException;
import com.google.api.gax.rpc.StatusCode;
import com.google.auth.oauth2.ServiceAccountCredentials;
import com.google.cloud.pubsub.v1.Publisher;
import com.google.cloud.pubsub.v1.TopicAdminClient;
import com.google.cloud.pubsub.v1.TopicAdminSettings;
import com.google.common.base.Preconditions;
import com.google.common.io.BaseEncoding;
import com.google.pubsub.v1.ProjectTopicName;
import com.segment.backo.Backo;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.threeten.bp.Duration;

/** Create publisher with the same config. Auto create topic if not exists. */
public class PublisherFactoryImpl implements PublisherFactory {
  static final int NUM_RETRIES_ATTEMPT = 8;
  private static final Logger LOGGER = LoggerFactory.getLogger(PublisherFactoryImpl.class);
  // 10, 20, 40, 80, 160, 320, 640, 1280, 2560, 3000, 3000, ...
  private static final Backo BACKO =
      Backo.builder()
          .base(TimeUnit.MILLISECONDS, 10)
          .cap(TimeUnit.SECONDS, 3)
          .jitter(0)
          .factor(2)
          .build();

  private static final Duration MIN_TOTAL_TIMEOUT = Duration.ofSeconds(10);
  private static final Duration DEFAULT_TOTAL_TIMEOUT = MIN_TOTAL_TIMEOUT;
  private static final Duration DEFAULT_RPC_TIMEOUT = Duration.ofSeconds(10);
  // Pubsub has additional constraints to RetrySettings. This is copied from Publisher.Builder
  private static final RetrySettings DEFAULT_RETRY_SETTINGS =
      RetrySettings.newBuilder()
          .setTotalTimeout(DEFAULT_TOTAL_TIMEOUT)
          .setInitialRetryDelay(Duration.ofMillis(5))
          .setRetryDelayMultiplier(2)
          .setMaxRetryDelay(Duration.ofMillis(Long.MAX_VALUE))
          .setInitialRpcTimeout(DEFAULT_RPC_TIMEOUT)
          .setRpcTimeoutMultiplier(2)
          .setMaxRpcTimeout(DEFAULT_RPC_TIMEOUT)
          .build();
  // 50, 100, 200, 400, 800, 1600|, 3200, 5000, 5000, ...
  // 50, 150, 350, 750, 1550, 3150, 6350, 11350
  private static final RetrySettings retrySettings =
      DEFAULT_RETRY_SETTINGS
          .toBuilder()
          .setInitialRetryDelay(Duration.ofMillis(50))
          .setRetryDelayMultiplier(2)
          .setMaxRetryDelay(Duration.ofSeconds(5))
          .setMaxAttempts(5)
          .build();

  private static final BatchingSettings batchingSettings =
      BatchingSettings.newBuilder()
          .setDelayThreshold(Duration.ofMillis(50))
          .setRequestByteThreshold(500_000L)
          .setElementCountThreshold(5000L)
          .build();

  private final String projectId;
  private final TopicAdminClient topicAdminClient;
  private final ExecutorProviderKind executorProviderKind;
  private final List<Publisher> publishers = new ArrayList<>();

  // will use default auth if null.
  private CredentialsProvider credentialsProvider = null;
  // in case we use SharedExecutor, this class manages shared executor shutdown.
  private ScheduledExecutorService sharedExecutor = null;

  PublisherFactoryImpl(String projectId, TopicAdminClient topicAdminClient) throws IOException {
    this.projectId = projectId;
    this.topicAdminClient = topicAdminClient;
    this.executorProviderKind = new PerPublisherExecutor(2);
  }

  private PublisherFactoryImpl(Builder builder) throws IOException {
    this.projectId = builder.projectId;
    if (builder.privateKeyBase64 != null) {
      byte[] decoded = BaseEncoding.base64().decode(builder.privateKeyBase64);

      ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream(decoded);
      this.credentialsProvider =
          FixedCredentialsProvider.create(
              ServiceAccountCredentials.fromStream(byteArrayInputStream));
    }

    TopicAdminSettings.Builder topicAdminBuilder = TopicAdminSettings.newBuilder();
    if (credentialsProvider != null) {
      topicAdminBuilder.setCredentialsProvider(credentialsProvider);
    }
    this.topicAdminClient = TopicAdminClient.create(topicAdminBuilder.build());

    this.executorProviderKind = builder.executorProviderKind;
    if (builder.executorProviderKind instanceof SharedExecutor) {
      this.sharedExecutor = ((SharedExecutor) builder.executorProviderKind).getExecutor();
    }
  }

  private void createTopicIfNotExist(String topicNameStr) throws IOException {
    ProjectTopicName topicName = ProjectTopicName.of(this.projectId, topicNameStr);
    for (int attempt = 0; attempt < NUM_RETRIES_ATTEMPT; attempt++) {
      try {
        this.topicAdminClient.createTopic(topicName);
        LOGGER.info("Topic created: {}", topicName);
        break;
      } catch (ApiException e) {
        if (isAlreadyExistsException(e)) {
          // it's a normal thing
          break;

        } else if (isUnauthenticatedException(e) || isInternalException(e)) {
          if (attempt + 1 == NUM_RETRIES_ATTEMPT) { // after last attempt, throw exception
            LOGGER.error(
                "Retries exhausted after {} when creating topic {}",
                NUM_RETRIES_ATTEMPT,
                topicName);
            throw new IllegalStateException(
                "Failed to create topic '"
                    + topicNameStr
                    + "' after "
                    + NUM_RETRIES_ATTEMPT
                    + " attempts",
                e);
          }

          try {
            BACKO.sleep(attempt);
          } catch (InterruptedException e1) {
            throw new IllegalStateException(e);
          }

        } else {
          // unexpected error
          LOGGER.error("Can't create topic: " + topicName, e);
          throw e;
        }
      }
    }
  }

  private boolean isApiExceptionOfType(ApiException e, StatusCode.Code type) {
    if (e.getStatusCode() instanceof GrpcStatusCode) {
      GrpcStatusCode statusCode = (GrpcStatusCode) e.getStatusCode();
      return statusCode.getCode() == type;
    }
    return false;
  }

  private boolean isUnauthenticatedException(ApiException e) {
    return isApiExceptionOfType(e, StatusCode.Code.UNAUTHENTICATED);
  }

  private boolean isAlreadyExistsException(ApiException e) {
    return isApiExceptionOfType(e, StatusCode.Code.ALREADY_EXISTS);
  }

  private boolean isInternalException(ApiException e) {
    return isApiExceptionOfType(e, StatusCode.Code.INTERNAL);
  }

  @Override
  public Publisher createPublisher(String topic) throws IOException {
    ProjectTopicName topicName = ProjectTopicName.of(this.projectId, topic);
    createTopicIfNotExist(topic);

    Publisher.Builder builder =
        Publisher.newBuilder(topicName)
            .setExecutorProvider(executorProviderKind.getExecutorProvider())
            .setRetrySettings(retrySettings)
            .setBatchingSettings(batchingSettings);
    if (this.credentialsProvider != null) {
      builder.setCredentialsProvider(this.credentialsProvider);
    }

    LOGGER.info("Publisher for {} is created.", topicName);

    Publisher publisher = builder.build();
    this.publishers.add(publisher);

    return publisher;
  }

  @Override
  public void shutdown() {
    for (Publisher publisher : this.publishers) {
      try {
        publisher.shutdown();
      } catch (Exception e) {
        LOGGER.error("Error when shutting down publisher", e);
      }
    }

    topicAdminClient.shutdown();

    if (this.sharedExecutor != null) {
      this.sharedExecutor.shutdown();
      try {
        boolean done = this.sharedExecutor.awaitTermination(10, TimeUnit.SECONDS);
        LOGGER.info(
            "Is it 'all task completed' | 'timeout elapsed'? {}", (done ? "former" : "latter"));
      } catch (InterruptedException e) {
        LOGGER.error("Error when awaitTermination shared executor", e);
      }
    }
  }

  public static final class Builder {
    static final ExecutorProviderKind DEFAULT_EXECUTOR_PROVIDER_KIND = new PerPublisherExecutor(2);

    String projectId;
    String privateKeyBase64;
    List<String> initialTopics = Collections.emptyList();
    ExecutorProviderKind executorProviderKind = DEFAULT_EXECUTOR_PROVIDER_KIND;

    public Builder(String projectId) {
      this.projectId = projectId;
    }

    public Builder setPrivateKeyBase64(String privateKeyBase64) {
      Preconditions.checkNotNull(projectId);
      this.privateKeyBase64 = privateKeyBase64;
      return this;
    }

    public Builder setExecutorProviderKind(ExecutorProviderKind executorProviderKind) {
      this.executorProviderKind = executorProviderKind;
      return this;
    }

    public Builder setInitialTopics(List<String> initialTopics) {
      this.initialTopics = initialTopics;
      return this;
    }

    public PublisherFactoryImpl build() throws IOException {
      return new PublisherFactoryImpl(this);
    }
  }
}
