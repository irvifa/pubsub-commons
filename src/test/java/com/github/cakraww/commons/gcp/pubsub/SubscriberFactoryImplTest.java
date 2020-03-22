package com.github.cakraww.commons.gcp.pubsub;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.api.gax.grpc.GrpcStatusCode;
import com.google.api.gax.rpc.ApiException;
import com.google.api.gax.rpc.StatusCode;
import com.google.cloud.pubsub.v1.SubscriptionAdminClient;
import com.google.pubsub.v1.ProjectSubscriptionName;
import com.google.pubsub.v1.ProjectTopicName;
import com.google.pubsub.v1.PushConfig;
import com.google.pubsub.v1.Subscription;
import io.grpc.Status;
import java.io.IOException;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.junit.MockitoJUnitRunner;
import org.mockito.stubbing.Answer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@RunWith(MockitoJUnitRunner.class)
public class SubscriberFactoryImplTest {
  private final static Logger LOGGER = LoggerFactory.getLogger(SubscriberFactoryImplTest.class);
  private SubscriberFactoryImpl underTest;

  @Mock
  private SubscriptionAdminClient subscriptionAdminClientMock;

  @Before
  public void setup() throws Exception {
    underTest = new SubscriberFactoryImpl("test-project", subscriptionAdminClientMock);
  }

  @Test
  public void testCreateSubscriberAndSubscriptionIfNeeded_retryIfUnanthenticated() throws IOException {
    final AtomicLong attempt = new AtomicLong(0);
    when(subscriptionAdminClientMock.createSubscription(any(ProjectSubscriptionName.class),
        any(ProjectTopicName.class),
        any(PushConfig.class),
        any(Integer.class))).thenAnswer(
        new Answer<Subscription>() {
          @Override
          public Subscription answer(InvocationOnMock invocation) throws Throwable {
            if (attempt.get() == 0) {
              attempt.incrementAndGet();
              throw new ApiException(null, GrpcStatusCode.of(Status.Code.UNAUTHENTICATED), false);
            } else {
              return null;
            }
          }
        });

    underTest.createSubscriber("topicA", "subscriptionA", null);

    verify(subscriptionAdminClientMock, times(2))
        .createSubscription(
            ProjectSubscriptionName.of("test-project", "topicA-subscriptionA"),
            ProjectTopicName.of("test-project", "topicA"),
            PushConfig.getDefaultInstance(),
            10);
  }

  @Test
  public void testCreateSubscriberAndSubscription_retryIfNeeded() throws IOException {
    final AtomicLong attempt = new AtomicLong(0);
    when(subscriptionAdminClientMock.createSubscription(any(ProjectSubscriptionName.class),
        any(ProjectTopicName.class),
        any(PushConfig.class),
        any(Integer.class))).thenAnswer(
        new Answer<Subscription>() {
          @Override
          public Subscription answer(InvocationOnMock invocation) throws Throwable {
            if (attempt.get() == 0) {
              attempt.incrementAndGet();
              throw new ApiException(null, GrpcStatusCode.of(Status.Code.INTERNAL), false);
            } else {
              return null;
            }
          }
        });

    underTest.createSubscriber("topicA", "subscriptionA", null);

    verify(subscriptionAdminClientMock, times(2))
        .createSubscription(
            ProjectSubscriptionName.of("test-project", "topicA-subscriptionA"),
            ProjectTopicName.of("test-project", "topicA"),
            PushConfig.getDefaultInstance(),
            10);
  }

  @Test
  public void testCreateSubscriberAndSubscription_retriesExhausted_throwException() throws IOException {
    when(subscriptionAdminClientMock.createSubscription(any(ProjectSubscriptionName.class),
        any(ProjectTopicName.class),
        any(PushConfig.class),
        any(Integer.class))).thenAnswer(
        new Answer<Subscription>() {
          @Override
          public Subscription answer(InvocationOnMock invocation) throws Throwable {
            throw new ApiException(null, GrpcStatusCode.of(Status.Code.INTERNAL), false);
          }
        });

    try {
      underTest.createSubscriber("topicA", "subscriptionA", null);
      fail();
    } catch (IllegalStateException e) {
      LOGGER.info(e.getMessage());

      verify(subscriptionAdminClientMock, times(SubscriberFactoryImpl.NUM_OF_RETRIES_ATTEMPT))
          .createSubscription(
              ProjectSubscriptionName.of("test-project", "topicA-subscriptionA"),
              ProjectTopicName.of("test-project", "topicA"),
              PushConfig.getDefaultInstance(),
              10);
      assertThat(((ApiException) e.getCause()).getStatusCode().getCode()).
          isEqualTo(StatusCode.Code.INTERNAL);
    }
  }
}
