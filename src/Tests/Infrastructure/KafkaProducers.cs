// using Avro.Generic;

using Confluent.Kafka.SyncOverAsync;
using Confluent.SchemaRegistry;
using Confluent.SchemaRegistry.Serdes;
using Example;
using GR8Tech.TestUtils.KafkaClient.Abstractions;
using GR8Tech.TestUtils.KafkaClient.Clients.KafkaProducer;
using Tests.Helpers;

namespace Tests.Infrastructure;

public static class KafkaProducers
{
    public static readonly IKafkaProducer<string, string> SimpleStringString;
    public static readonly IKafkaProducer<string, SimpleClass> SimpleStringObject;
    public static readonly IKafkaProducer<string, SimpleAvroClass> SimpleStringAvroObject;
    public static readonly IKafkaProducer<string, SimpleProtobufClass> SimpleStringProtobufObject;

    static KafkaProducers()
    {
        var schemaRegistryClient = new CachedSchemaRegistryClient(
            new SchemaRegistryConfig
            {
                Url = "http://localhost:8081"
            });

        SimpleStringString = new KafkaProducer<string, string>("simple_string_string");
        SimpleStringObject = new KafkaProducer<string, SimpleClass>("simple_string_object");
        SimpleStringAvroObject = new KafkaProducer<string, SimpleAvroClass>("simple_string_avro_object",
            valueSerializer: new AvroSerializer<SimpleAvroClass>(schemaRegistryClient).AsSyncOverAsync());
        SimpleStringProtobufObject = new KafkaProducer<string, SimpleProtobufClass>("simple_string_protobuf_object",
            valueSerializer: new ProtobufSerializer<SimpleProtobufClass>(schemaRegistryClient).AsSyncOverAsync());
    }

    public static void DisposeAll()
    {
        SimpleStringString.Dispose();
        SimpleStringObject.Dispose();
        SimpleStringAvroObject.Dispose();
        SimpleStringProtobufObject.Dispose();
    }
}