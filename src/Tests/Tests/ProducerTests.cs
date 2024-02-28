using Example;
using GR8Tech.TestUtils.KafkaClient.Clients.KafkaProducer;
using GR8Tech.TestUtils.KafkaClient.Common.Logging;
using GR8Tech.TestUtils.KafkaClient.Settings;
using NUnit.Framework;
using Tests.Helpers;
using Tests.Infrastructure;
using Tests.SetUpFixtures;
using ILogger = Serilog.ILogger;

namespace Tests.Tests;

public class ProducerTests : TestBase
{
    private ILogger _logger = SerilogDecorator.Logger.ForContext<ProducerTests>();

    [SetUp]
    public void Setup()
    {
    }

    [Test]
    public async Task Produce_string_string_message()
    {
        // Arrange
        var key = GetNewGuid();
        var value = GetNewGuid();

        // Act 
        var result = await KafkaProducers.SimpleStringString.ProduceMessage(key, value);

        // Assert
        Assert.That(result, Is.Not.EqualTo(null!));
    }

    [Test]
    public async Task Produce_string_string_message_with_headers()
    {
        // Arrange
        var key = GetNewGuid();
        var value = GetNewGuid();
        var headers = new Dictionary<string, string>()
        {
            { "headerName", "headerValue" }
        };

        // Act 
        var result = await KafkaProducers.SimpleStringString.ProduceMessage(key, value, headers);

        // Assert
        Assert.That(result, Is.Not.EqualTo(null!));
        Assert.That(result.Message.Headers[0].Key == "headerName");
    }

    [Test]
    public async Task Produce_string_object_message()
    {
        // Arrange
        var key = GetNewGuid();

        var value = new SimpleClass
        {
            Id = RandomInt(),
            Name = GetNewGuid()
        };

        // Act 
        var result = await KafkaProducers.SimpleStringObject.ProduceMessage(key, value);

        // Assert
        Assert.That(result, Is.Not.EqualTo(null!));
    }

    [Test]
    public async Task Produce_string_avro_object_message()
    {
        // Arrange
        var key = GetNewGuid();
        var value = new SimpleAvroClass()
        {
            Id = RandomInt(),
            Name = GetNewGuid()
        };

        // Act  
        var result = await KafkaProducers.SimpleStringAvroObject.ProduceMessage(key, value);

        // Assert
        Assert.That(result, Is.Not.EqualTo(null!));
    }

    [Test]
    public async Task Produce_string_protobuf_object_message()
    {
        // Arrange
        var key = GetNewGuid();

        var value = new SimpleProtobufClass
        {
            Id = RandomInt(),
            Name = GetNewGuid()
        };

        // Act  
        var result = await KafkaProducers.SimpleStringProtobufObject.ProduceMessage(key, value);

        // Assert
        Assert.That(result, Is.Not.EqualTo(null!));
    }

    [Test]
    public async Task Produce_string_object_message_with_own_producer()
    {
        // Arrange
        var key = GetNewGuid();
        var value = new SimpleClass
        {
            Id = RandomInt(),
            Name = GetNewGuid()
        };

        var name = "my-own-producer";
        var topic = "my-own-producer-topic";
        var producer = CreateKafkaProducer<string, SimpleClass>(name, topic);

        // Act  
        var result = await producer.ProduceMessage(key, value);

        // Assert
        Assert.That(result, Is.Not.EqualTo(null!));
    }

    [Test]
    public void Try_to_create_two_equal_producers_when_this_is_forbidden()
    {
        // Arrange
        var name = "equal_producer";
        var topic = "equal_producer-topic";
        var producer1 = CreateKafkaProducer<string, string>(name, topic);

        // Act  
        try
        {
            var producer2 = CreateKafkaProducer<string, string>(name, topic);

            Assert.That(producer2, Is.Null);
        }
        catch (Exception e)
        {
            // Assert
            Assert.That(e.Message, Is.EqualTo("Kafka producer \"equal_producer\" already exists"));
        }
    }

    [Test]
    public void Try_to_create_dispose_and_create_again_the_same_producer()
    {
        // Arrange
        var name = "create_dispose_and_create_again_the_same_producer";
        var topic = "create_dispose_and_create_again_the_same_producer-topic";
        var producer = CreateKafkaProducer<string, string>(name, topic);

        // Act
        producer.Dispose();
        producer = CreateKafkaProducer<string, string>(name, topic);

        // Assert
        Assert.That(producer, Is.Not.Null);
    }

    private KafkaProducer<TKey, TValue> CreateKafkaProducer<TKey, TValue>(string name, string topic)
    {
        return new KafkaProducer<TKey, TValue>(name,
            new ProducerSettings
            {
                Topic = topic,
                Config = new Dictionary<string, string>
                {
                    { "bootstrap.servers", "localhost:9092" },
                    { "partitioner", "murmur2" },
                    { "acks", "all" },
                    { "message.timeout.ms", "20000" }
                }
            });
    }
}