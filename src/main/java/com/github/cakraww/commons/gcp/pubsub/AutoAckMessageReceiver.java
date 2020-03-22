package com.github.cakraww.commons.gcp.pubsub;

import com.google.cloud.pubsub.v1.AckReplyConsumer;
import com.google.cloud.pubsub.v1.MessageReceiver;
import com.google.pubsub.v1.PubsubMessage;

abstract class AutoAckMessageReceiver<T> implements MessageReceiver {
  public abstract void onMessageReceived(T message);

  abstract T transformMessage(PubsubMessage message);

  @Override
  public void receiveMessage(PubsubMessage message, AckReplyConsumer consumer) {
    T transformed = transformMessage(message);
    consumer.ack();
    onMessageReceived(transformed);
  }
}
