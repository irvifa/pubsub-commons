package com.github.cakraww.commons.gcp.pubsub;

import com.google.cloud.pubsub.v1.MessageReceiver;
import com.google.cloud.pubsub.v1.Subscriber;

public interface SubscriberFactory {
  Subscriber createSubscriber(String topicName, String consumerId, MessageReceiver receiver);

  void shutdown();
}
