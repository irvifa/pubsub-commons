package com.github.cakraww.commons.gcp.pubsub;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.cloud.pubsub.v1.Publisher;
import com.google.common.collect.ImmutableMap;
import com.google.protobuf.ByteString;
import com.google.pubsub.v1.PubsubMessage;
import java.io.IOException;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class MultiTopicPublisherTest {
  @Mock PublisherFactory publisherFactoryMock;

  MultiTopicPublisher underTest;

  @Before
  public void setUp() throws Exception {
    underTest = new MultiTopicPublisher(publisherFactoryMock);
  }

  @Test
  public void testPublish_publisherAlreadyInCache() throws Exception {
    Publisher publisherMock = mock(Publisher.class);
    underTest.publisherLoadingCache.asMap().put("topicA", publisherMock);

    underTest.publish("topicA", "payload", null);

    verify(publisherMock, times(1)).publish(any(PubsubMessage.class));
    verify(publisherFactoryMock, times(0)).createPublisher(anyString());
  }

  @Test
  public void testPublish_publisherCacheMiss_createPublisher() throws IOException {
    Publisher publisherMock = mock(Publisher.class);
    when(publisherFactoryMock.createPublisher("topicA")).thenReturn(publisherMock);

    underTest.publish("topicA", "payload", null);

    verify(publisherMock, times(1)).publish(any(PubsubMessage.class));
    verify(publisherFactoryMock, times(1)).createPublisher(anyString());
  }

  @Test
  public void publish() throws Exception {
    Publisher publisherMock = mock(Publisher.class);
    when(publisherFactoryMock.createPublisher("topicA")).thenReturn(publisherMock);

    underTest.publish("topicA", "payload", ImmutableMap.of("meta", "data"));

    ArgumentCaptor<PubsubMessage> messageCaptor = ArgumentCaptor.forClass(PubsubMessage.class);
    verify(publisherMock, times(1)).publish(messageCaptor.capture());

    PubsubMessage pubsubMessage = messageCaptor.getValue();
    assertThat(pubsubMessage.getData()).isEqualTo(ByteString.copyFromUtf8("payload"));
    assertThat(pubsubMessage.getAttributesMap()).isEqualTo(ImmutableMap.of("meta", "data"));
  }
}
