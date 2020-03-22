package com.github.cakraww.commons.gcp.pubsub;

import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Message;
import com.google.protobuf.Parser;
import com.google.pubsub.v1.PubsubMessage;

public abstract class ProtobufAutoAckMessageReceiver<T extends Message> extends AutoAckMessageReceiver<T> {
  private final Parser<T> parser;

  public ProtobufAutoAckMessageReceiver(Parser<T> parser) {
    this.parser = parser;
  }

  @Override
  T transformMessage(PubsubMessage message) {
    ByteString decoded = message.getData();
    return readProtobufMessage(decoded);
  }

  private T readProtobufMessage(ByteString data) {
    try {
      T message = parser.parseFrom(data);
      return message;
    } catch (InvalidProtocolBufferException e) {
      throw new IllegalStateException(e);
    }
  }
}
