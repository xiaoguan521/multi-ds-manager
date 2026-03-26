package com.example.multids.client;

import com.example.multids.grpc.v1.DynamicDataSourceGrpc;
import com.example.multids.grpc.v1.ExecuteRequest;
import com.example.multids.grpc.v1.ExecuteResponse;
import com.example.multids.grpc.v1.OperationType;
import com.example.multids.grpc.v1.PingRequest;
import com.example.multids.grpc.v1.PingResponse;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import java.util.concurrent.TimeUnit;

public final class ClientMain {
  private ClientMain() {}

  public static void main(String[] args) throws InterruptedException {
    ManagedChannel channel =
        ManagedChannelBuilder.forAddress("127.0.0.1", 50051)
            .usePlaintext()
            .build();

    try {
      DynamicDataSourceGrpc.DynamicDataSourceBlockingStub stub =
          DynamicDataSourceGrpc.newBlockingStub(channel);

      PingResponse ping = stub.ping(PingRequest.newBuilder().build());
      System.out.println(
          "Ping: " + ping.getMessage() + " datasources=" + ping.getDatasourceCount());

      ExecuteRequest request =
          ExecuteRequest.newBuilder()
              .setJgbh("340100")
              .setOperationType(OperationType.QUERY)
              .setSql("SELECT 1 AS test")
              .setCallerId("bootstrap-client")
              .setAuthToken("bootstrap-secret")
              .setRequestId("java-demo-001")
              .setOperator("bootstrap")
              .setMaxRows(10)
              .build();

      ExecuteResponse response = stub.execute(request);
      System.out.println("Execute success: " + response.getSuccess());
      System.out.println(
          "Datasource: " + response.getDatasourceName() + " " + response.getBackend());
      System.out.println("Rows: " + response.getRowsList());
    } finally {
      channel.shutdownNow().awaitTermination(5, TimeUnit.SECONDS);
    }
  }
}
