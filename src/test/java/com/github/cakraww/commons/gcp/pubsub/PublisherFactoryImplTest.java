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
import com.google.cloud.pubsub.v1.TopicAdminClient;
import com.google.pubsub.v1.ProjectTopicName;
import com.google.pubsub.v1.Topic;
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
public class PublisherFactoryImplTest {
  private static final Logger LOGGER = LoggerFactory.getLogger(PublisherFactoryImplTest.class);

  PublisherFactoryImpl underTest;
  @Mock
  TopicAdminClient topicAdminClientMock;

  @Before
  public void setUp() throws Exception {
    underTest = new PublisherFactoryImpl("test-project", topicAdminClientMock);
  }

  @Test
  public void testCreatePublisherAndTopicIfNeeded_retryIfUnanthenticated() throws IOException {
    final AtomicLong attempt = new AtomicLong(0);
    when(topicAdminClientMock.createTopic(any(ProjectTopicName.class)))
        .thenAnswer(new Answer<Topic>() {
          @Override
          public Topic answer(InvocationOnMock invocation) throws Throwable {
            if (attempt.get() == 0) {
              attempt.incrementAndGet();
              throw new ApiException(null, GrpcStatusCode.of(Status.Code.UNAUTHENTICATED), false);
            } else {
              return null;
            }
          }
        });

    underTest.createPublisher("topicA");
    verify(topicAdminClientMock, times(2))
        .createTopic(ProjectTopicName.of("test-project", "topicA"));
  }

  @Test
  public void testCreatePublisherAndTopicIfNeeded_retryIfInternal() throws IOException {
    final AtomicLong attempt = new AtomicLong(0);
    when(topicAdminClientMock.createTopic(any(ProjectTopicName.class)))
        .thenAnswer(new Answer<Topic>() {
          @Override
          public Topic answer(InvocationOnMock invocation) throws Throwable {
            if (attempt.get() == 0) {
              attempt.incrementAndGet();
              throw new ApiException(null, GrpcStatusCode.of(Status.Code.INTERNAL), false);
            } else {
              return null;
            }
          }
        });

    underTest.createPublisher("topicA");
    verify(topicAdminClientMock, times(2))
        .createTopic(ProjectTopicName.of("test-project", "topicA"));
  }

  @Test
  public void testCreatePublisherAndTopicIfNeeded_retriesExhausted_throwException() throws IOException {
    when(topicAdminClientMock.createTopic(any(ProjectTopicName.class)))
        .thenAnswer(new Answer<Topic>() {
          @Override
          public Topic answer(InvocationOnMock invocation) throws Throwable {
            throw new ApiException(null, GrpcStatusCode.of(Status.Code.INTERNAL), false);
          }
        });

    try {
      underTest.createPublisher("topicA");
      fail();
    } catch (IllegalStateException e) {
      LOGGER.info(e.getMessage());

      verify(topicAdminClientMock, times(PublisherFactoryImpl.NUM_RETRIES_ATTEMPT))
          .createTopic(ProjectTopicName.of("test-project", "topicA"));
      assertThat(((ApiException) e.getCause()).getStatusCode().getCode()).
          isEqualTo(StatusCode.Code.INTERNAL);
    }
  }
}
