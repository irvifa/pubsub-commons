package com.github.cakraww.commons.gcp.pubsub.core;

import com.google.api.gax.core.ExecutorProvider;
import com.google.api.gax.core.FixedExecutorProvider;
import java.util.concurrent.ScheduledExecutorService;

/** One executor shared for all publisher. */
public class SharedExecutor implements ExecutorProviderKind {
  private final ExecutorProvider executorProvider;
  private final ScheduledExecutorService executor;

  public SharedExecutor(ScheduledExecutorService executor) {
    this.executor = executor;
    this.executorProvider = FixedExecutorProvider.create(executor);
  }

  @Override
  public ExecutorProvider getExecutorProvider() {
    return this.executorProvider;
  }

  public ScheduledExecutorService getExecutor() {
    return executor;
  }
}
