package com.github.cakraww.commons.gcp.pubsub.core;

import com.google.api.gax.core.ExecutorProvider;
import com.google.api.gax.core.InstantiatingExecutorProvider;

/** One executor for each publisher. */
public class PerPublisherExecutor implements ExecutorProviderKind {
  private final InstantiatingExecutorProvider executorProvider;

  public PerPublisherExecutor(int threadPerPublisher) {
    this.executorProvider =
        InstantiatingExecutorProvider.newBuilder()
            .setExecutorThreadCount(threadPerPublisher)
            .build();
  }

  @Override
  public ExecutorProvider getExecutorProvider() {
    return this.executorProvider;
  }
}
