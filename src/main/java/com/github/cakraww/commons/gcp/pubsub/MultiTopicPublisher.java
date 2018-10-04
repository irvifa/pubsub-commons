package com.github.cakraww.commons.gcp.pubsub;

import com.google.api.core.ApiFuture;
import com.google.cloud.pubsub.v1.Publisher;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.ImmutableMap;
import com.google.protobuf.ByteString;
import com.google.pubsub.v1.PubsubMessage;
import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Publisher that can publish to multiple topics. Caching multiple publishers. Never expires. */
public class MultiTopicPublisher {
  private static final Logger LOGGER = LoggerFactory.getLogger(MultiTopicPublisher.class);

  private final PublisherFactory publisherFactory;
  final LoadingCache<String, Publisher> publisherLoadingCache =
      CacheBuilder.newBuilder()
          .build(
              new CacheLoader<String, Publisher>() {
                @Override
                public Publisher load(String topicName) throws Exception {
                  return publisherFactory.createPublisher(topicName);
                }
              });

  public MultiTopicPublisher(PublisherFactory publisherFactory) throws IOException {
    this(publisherFactory, Collections.<String>emptyList());
  }

  public MultiTopicPublisher(PublisherFactory publisherFactory, List<String> initialTopics)
      throws IOException {
    this.publisherFactory = publisherFactory;

    int totalTopic = initialTopics.size();
    LOGGER.info("Initializing " + totalTopic + " pubsub publishers.");
    int currentId = 1;
    for (String topicNameStr : initialTopics) {
      Publisher publisher = publisherFactory.createPublisher(topicNameStr);
      publisherLoadingCache.put(topicNameStr, publisher);
      LOGGER.info("Completed " + (currentId++) + "/" + totalTopic);
    }
  }

  public ApiFuture<String> publish(String topic, String payload, Map<String, String> metadata) {
    if (metadata == null) {
      metadata = ImmutableMap.of();
    }

    Publisher publisher = publisherLoadingCache.getUnchecked(topic);

    PubsubMessage pubsubMessage =
        PubsubMessage.newBuilder()
            .setData(ByteString.copyFromUtf8(payload))
            .putAllAttributes(metadata)
            .build();

    return publisher.publish(pubsubMessage);
  }

  public void shutdown() {
    publisherFactory.shutdown();
  }
}
