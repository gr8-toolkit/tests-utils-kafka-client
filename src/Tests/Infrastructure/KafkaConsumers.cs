// using Avro.Generic;

using Confluent.Kafka.SyncOverAsync;
using Confluent.SchemaRegistry;
using Confluent.SchemaRegistry.Serdes;
using Example;
using GR8Tech.TestUtils.KafkaClient.Abstractions;
using GR8Tech.TestUtils.KafkaClient.Clients.KafkaConsumer;
using Tests.Helpers;

namespace Tests.Infrastructure;

public static class KafkaConsumers
{
    public static readonly IKafkaConsumer<string, string> SimpleStringString;
    public static readonly IKafkaConsumer<string, SimpleClass> SimpleStringObject;
    public static readonly IKafkaConsumer<string, SimpleAvroClass> SimpleStringAvroObject;
    public static readonly IKafkaConsumer<string, SimpleProtobufClass> SimpleStringProtobufObject;

    static KafkaConsumers()
    {
        var schemaRegistryClient = new CachedSchemaRegistryClient(
            new SchemaRegistryConfig
            {
                Url = "http://localhost:8081"
            });

        SimpleStringString = new KafkaConsumer<string, string>("simple_string_string");
        SimpleStringObject = new KafkaConsumer<string, SimpleClass>("simple_string_object");
        SimpleStringAvroObject = new KafkaConsumer<string, SimpleAvroClass>("simple_string_avro_object",
            valueDeserializer: new AvroDeserializer<SimpleAvroClass>(schemaRegistryClient).AsSyncOverAsync());
        SimpleStringProtobufObject = new KafkaConsumer<string, SimpleProtobufClass>("simple_string_protobuf_object",
            valueDeserializer: new ProtobufDeserializer<SimpleProtobufClass>().AsSyncOverAsync());
    }

    public static void StartAll()
    {
        var tasksList = new List<Task>();
        
        tasksList.Add(Task.Run(() => SimpleStringString.Start()));
        tasksList.Add(Task.Run(() => SimpleStringObject.Start()));
        tasksList.Add(Task.Run(() => SimpleStringAvroObject.Start()));
        tasksList.Add(Task.Run(() => SimpleStringProtobufObject.Start()));
        
        Task.WaitAll(tasksList.ToArray());
    }

    public static void DisposeAll()
    {
        var tasksList = new List<Task>();
        
        tasksList.Add(Task.Run(() => SimpleStringString.Dispose()));
        tasksList.Add(Task.Run(() => SimpleStringObject.Dispose()));
        tasksList.Add(Task.Run(() => SimpleStringAvroObject.Dispose()));
        tasksList.Add(Task.Run(() => SimpleStringProtobufObject.Dispose()));
        
        Task.WaitAll(tasksList.ToArray());
    }
}