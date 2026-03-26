const path = require("path");
const grpc = require("@grpc/grpc-js");
const protoLoader = require("@grpc/proto-loader");

const protoPath = path.resolve(__dirname, "../../proto/dynamic_ds.proto");
const packageDefinition = protoLoader.loadSync(protoPath, {
  keepCase: false,
  longs: String,
  enums: String,
  defaults: true,
  oneofs: true,
});
const proto = grpc.loadPackageDefinition(packageDefinition).multi_ds.grpc.v1;

const client = new proto.DynamicDataSource(
  "127.0.0.1:50051",
  grpc.credentials.createInsecure(),
);

function ping() {
  return new Promise((resolve, reject) => {
    client.Ping({}, (error, response) => {
      if (error) {
        reject(error);
        return;
      }
      resolve(response);
    });
  });
}

function execute() {
  return new Promise((resolve, reject) => {
    client.Execute(
      {
        jgbh: "340100",
        operationType: "QUERY",
        sql: "SELECT 1 AS test",
        callerId: "bootstrap-client",
        authToken: "bootstrap-secret",
        requestId: "node-demo-001",
        operator: "bootstrap",
        maxRows: 10,
      },
      (error, response) => {
        if (error) {
          reject(error);
          return;
        }
        resolve(response);
      },
    );
  });
}

async function main() {
  const pingResponse = await ping();
  console.log("Ping:", pingResponse.message, "datasources=", pingResponse.datasourceCount);

  const executeResponse = await execute();
  console.log("Execute success:", executeResponse.success);
  console.log(
    "Datasource:",
    executeResponse.datasourceName,
    executeResponse.backend,
  );
  console.log("Rows:", JSON.stringify(executeResponse.rows, null, 2));
}

main().catch((error) => {
  console.error("gRPC call failed:", error);
  process.exitCode = 1;
});
