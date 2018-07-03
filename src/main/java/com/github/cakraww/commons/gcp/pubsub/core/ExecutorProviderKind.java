package com.github.cakraww.commons.gcp.pubsub.core;

import com.google.api.gax.core.ExecutorProvider;

/**
 * Wrapper for ExecutorProvider. Mainly to avoid direct dependency to gax.
 */
public interface ExecutorProviderKind {
  ExecutorProvider getExecutorProvider();
}
