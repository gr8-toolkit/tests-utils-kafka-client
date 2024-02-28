using Ductus.FluentDocker.Builders;
using Ductus.FluentDocker.Services;

namespace Tests.SetUpFixtures;

public class DockerFixture : IDisposable
{
    private readonly ICompositeService _containers;

    public DockerFixture()
    {
        _containers = new Builder()
            //Zookeeper
            .UseContainer()
            .UseImage("wurstmeister/zookeeper:latest")
            .WithHostName("zookeeper")
            .WithName("zookeeper")
            .ExposePort(2181, 2181)
            .WaitForPort("2181/tcp", 30000 /*30s*/)
            .WaitForMessageInLog("binding to port", TimeSpan.FromSeconds(15))
            .Builder()

            //Kafka
            .UseContainer()
            .UseImage("wurstmeister/kafka:latest")
            .WithHostName("kafka")
            .WithName("kafka")
            .Link("zookeeper")
            .ExposePort(9092, 9092)
            .ExposePort(9094, 9094)
            .WithEnvironment(
                "KAFKA_ZOOKEEPER_CONNECT=zookeeper:2181",
                "KAFKA_ADVERTISED_LISTENERS=INSIDE://kafka:9094,OUTSIDE://localhost:9092",
                "KAFKA_LISTENERS=INSIDE://:9094,OUTSIDE://:9092",
                "KAFKA_LISTENER_SECURITY_PROTOCOL_MAP=INSIDE:PLAINTEXT,OUTSIDE:PLAINTEXT",
                "KAFKA_INTER_BROKER_LISTENER_NAME=INSIDE")
            .WaitForPort("9092/tcp", 30000 /*30s*/)
            .WaitForMessageInLog("started (kafka.server.KafkaServer)", TimeSpan.FromSeconds(15))
            .Builder()

            //SchemaRegistry
            .UseContainer()
            .UseImage(" confluentinc/cp-schema-registry:latest")
            .WithHostName("schema-registry")
            .WithName("schema-registry")
            .Link("kafka")
            .WithEnvironment(
                "SCHEMA_REGISTRY_KAFKASTORE_BOOTSTRAP_SERVERS=kafka:9094",
                "SCHEMA_REGISTRY_HOST_NAME=schema-registry",
                "SCHEMA_REGISTRY_LISTENERS=http://schema-registry:8081")
            .ExposePort(8081, 8081)
            .WaitForPort("8081/tcp", 30000 /*30s*/)
            .WaitForMessageInLog("Server started, listening for requests", TimeSpan.FromSeconds(15))
            .Builder()

            // Run containers
            .Build()
            .Start();
    }

    public void Dispose()
    {
        _containers.Dispose();
    }
}