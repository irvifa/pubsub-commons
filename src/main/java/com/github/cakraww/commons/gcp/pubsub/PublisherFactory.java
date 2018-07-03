package com.github.cakraww.commons.gcp.pubsub;

import com.google.cloud.pubsub.v1.Publisher;
import java.io.IOException;

public interface PublisherFactory {
  Publisher createPublisher(String topic) throws IOException;

  void shutdown();
}
