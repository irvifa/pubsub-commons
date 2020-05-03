package com.github.cakraww.commons.gcp.pubsub;

import com.google.api.gax.core.CredentialsProvider;
import com.google.api.gax.core.FixedCredentialsProvider;
import com.google.api.gax.rpc.ApiException;
import com.google.auth.oauth2.ServiceAccountCredentials;
import com.google.cloud.pubsub.v1.MessageReceiver;
import com.google.cloud.pubsub.v1.Subscriber;
import com.google.cloud.pubsub.v1.SubscriptionAdminClient;
import com.google.cloud.pubsub.v1.SubscriptionAdminSettings;
import com.google.pubsub.v1.ProjectSubscriptionName;
import com.google.pubsub.v1.ProjectTopicName;
import com.google.pubsub.v1.PushConfig;
import com.segment.backo.Backo;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SubscriberFactoryImpl implements SubscriberFactory {
  static final int NUM_OF_RETRIES_ATTEMPT = 8;
  private static final String DELIMITER = "-";
  private static final Logger LOGGER = LoggerFactory.getLogger(SubscriberFactoryImpl.class);
  private static final Backo BACKO = Backo.builder()
      .base(TimeUnit.MILLISECONDS, 10)
      .cap(TimeUnit.SECONDS, 3)
      .jitter(0)
      .factor(2)
      .build();

  private final String projectId;
  private final SubscriptionAdminClient subscriptionAdminClient;
  private final ConcurrentMap<String, Subscriber> subscribers = new ConcurrentHashMap<>();
  private CredentialsProvider credentialsProvider = null;

  public SubscriberFactoryImpl(String projectId,
      SubscriptionAdminClient subscriptionAdminClient) {
    this.projectId = projectId;
    this.subscriptionAdminClient = subscriptionAdminClient;
  }

  private SubscriberFactoryImpl(Builder builder) throws IOException {
    this.projectId = builder.projectId;

    if (builder.privateKeyContent != null) {
      ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream(
          builder.privateKeyContent.getBytes("UTF-8")
      );
      this.credentialsProvider = FixedCredentialsProvider.create(
          ServiceAccountCredentials.fromStream(byteArrayInputStream));
    }

    SubscriptionAdminSettings.Builder subscriptionBuilder = SubscriptionAdminSettings.newBuilder();

    if (this.credentialsProvider != null) {
      subscriptionBuilder.setCredentialsProvider(this.credentialsProvider);
    }

    this.subscriptionAdminClient = SubscriptionAdminClient.create(subscriptionBuilder.build());
  }

  public String getSubscriptionName(String topicName, String consumerId) {
    return topicName + DELIMITER + consumerId;
  }

  private void createSubscriptionIfNotExist(String topicName, String consumerId) {
    ProjectSubscriptionName subscriptionName = ProjectSubscriptionName.of(this.projectId,
        getSubscriptionName(topicName, consumerId));
    ProjectTopicName projectTopicName = ProjectTopicName.of(this.projectId, topicName);

    for (int attempt = 0; attempt < NUM_OF_RETRIES_ATTEMPT; attempt++) {
      try {
        // 10 is the default ack deadline when we create a subscription
        this.subscriptionAdminClient.createSubscription(subscriptionName,
            projectTopicName,
            PushConfig.getDefaultInstance(),
            10);
        LOGGER.info("Subscription with consumer id {} for topic {} is created.", consumerId, topicName);
        break;
      } catch (ApiException e) {
        if (Helper.isAlreadyExistsException(e)) {
          // normal thing, since the subscription is already exists
          break;
        } else if (Helper.isUnauthenticatedException(e) || Helper.isInternalException(e)) {
          if (attempt + 1 == NUM_OF_RETRIES_ATTEMPT) { // after last attempt, throw exception
            LOGGER.error("Retries exhausted after {} when creating subscription {}", NUM_OF_RETRIES_ATTEMPT,
                projectTopicName);
            throw new IllegalStateException(
                "Failed to create subscription with consumer id '" + consumerId
                    + "' for topic id '" + topicName + " after " + NUM_OF_RETRIES_ATTEMPT
                    + " attempts", e);
          }

          try {
            BACKO.sleep(attempt);
          } catch (InterruptedException e1) {
            throw new IllegalStateException(e);
          }
        } else {
          LOGGER.error("Cant create subscription with consumer id " + consumerId + " for topic " + topicName, e);
          throw e;
        }
      }
    }
  }

  @Override
  public Subscriber createSubscriber(String topicName, String consumerId, MessageReceiver receiver) {
    String subscriptionName = getSubscriptionName(topicName, consumerId);
    ProjectSubscriptionName projectSubscriptionName = ProjectSubscriptionName.of(this.projectId, subscriptionName);
    createSubscriptionIfNotExist(topicName, consumerId);

    Subscriber.Builder builder = Subscriber.newBuilder(projectSubscriptionName, receiver);

    if (this.credentialsProvider != null) {
      builder.setCredentialsProvider(this.credentialsProvider);
    }

    Subscriber subscriber = builder.build();
    this.subscribers.put(consumerId, subscriber);
    subscriber.startAsync();

    LOGGER.info("Subscription with consumer id {} for topic {} is created.", consumerId, topicName);

    return subscriber;
  }

  @Override
  public void shutdown() {
    for (Subscriber subscriber : this.subscribers.values()) {
      try {
        subscriber.stopAsync()
            .awaitTerminated(10, TimeUnit.SECONDS);
      } catch (Exception e) {
        LOGGER.error("Can't shut down subscriber ", e);
      }
    }

    this.subscriptionAdminClient.shutdown();
  }

  public static final class Builder {
    String projectId;
    String privateKeyContent;

    public Builder(String projectId) {
      this.projectId = projectId;
    }

    public Builder setPrivateKeyContent(String privateKeyContent) {
      this.privateKeyContent = privateKeyContent;
      return this;
    }

    public SubscriberFactoryImpl build() throws IOException {
      return new SubscriberFactoryImpl(this);
    }
  }

}
