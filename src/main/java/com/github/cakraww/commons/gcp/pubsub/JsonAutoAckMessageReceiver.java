package com.github.cakraww.commons.gcp.pubsub;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import com.google.pubsub.v1.PubsubMessage;
import java.lang.reflect.Type;
import java.util.Map;

public abstract class JsonAutoAckMessageReceiver extends AutoAckMessageReceiver<Map<String, Object>> {
  private static final Gson GSON = new Gson();

  @Override
  Map<String, Object> transformMessage(PubsubMessage message) {
    Type type = new TypeToken<Map<String, Object>>() {
    }.getType();
    Map<String, Object> result = GSON.fromJson(message.getData().toStringUtf8(), type);
    return result;
  }
}
