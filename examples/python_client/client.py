import asyncio

import grpc

from dynamic_ds_pb2 import ExecuteRequest, PingRequest, QUERY
from dynamic_ds_pb2_grpc import DynamicDataSourceStub


async def main():
    async with grpc.aio.insecure_channel("127.0.0.1:50051") as channel:
        stub = DynamicDataSourceStub(channel)

        ping = await stub.Ping(PingRequest())
        print("Ping:", ping.message, "datasources=", ping.datasource_count)

        request = ExecuteRequest(
            jgbh="340100",
            operation_type=QUERY,
            sql="SELECT 1 AS test",
            request_id="python-demo-001",
            operator="bootstrap",
            caller_id="bootstrap-client",
            auth_token="bootstrap-secret",
            max_rows=10,
        )

        response = await stub.Execute(request)
        print("Execute success:", response.success)
        print("Datasource:", response.datasource_name, response.backend)
        print("Rows:", response.rows)


if __name__ == "__main__":
    asyncio.run(main())
