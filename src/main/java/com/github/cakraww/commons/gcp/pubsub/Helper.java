package com.github.cakraww.commons.gcp.pubsub;

import com.google.api.gax.grpc.GrpcStatusCode;
import com.google.api.gax.rpc.ApiException;
import com.google.api.gax.rpc.StatusCode;

final class Helper {
  private Helper() {
  }

  static boolean isApiExceptionOfType(ApiException e, StatusCode.Code type) {
    if (e.getStatusCode() instanceof GrpcStatusCode) {
      GrpcStatusCode statusCode = (GrpcStatusCode) e.getStatusCode();
      return statusCode.getCode() == type;
    }
    return false;
  }

  static boolean isUnauthenticatedException(ApiException e) {
    return isApiExceptionOfType(e, StatusCode.Code.UNAUTHENTICATED);
  }

  static boolean isAlreadyExistsException(ApiException e) {
    return isApiExceptionOfType(e, StatusCode.Code.ALREADY_EXISTS);
  }

  static boolean isInternalException(ApiException e) {
    return isApiExceptionOfType(e, StatusCode.Code.INTERNAL);
  }
}
