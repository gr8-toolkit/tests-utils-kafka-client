using Example;
using GR8Tech.TestUtils.KafkaClient.Clients.KafkaAdminClient;
using GR8Tech.TestUtils.KafkaClient.Clients.KafkaConsumer;
using GR8Tech.TestUtils.KafkaClient.Clients.KafkaProducer;
using GR8Tech.TestUtils.KafkaClient.Common.Logging;
using GR8Tech.TestUtils.KafkaClient.Settings;
using NUnit.Framework;
using Tests.Helpers;
using Tests.Infrastructure;
using Tests.SetUpFixtures;
using ILogger = Serilog.ILogger;

namespace Tests.Tests;

public class ConsumerTests : TestBase
{
    private ILogger _logger = SerilogDecorator.Logger.ForContext<ConsumerTests>();

    [SetUp]
    public void Setup()
    {
    }

    [Test]
    public async Task Consume_string_string_message()
    {
        // Arrange
        var key = GetNewGuid();
        var value = GetNewGuid();

        // Act  
        await KafkaProducers.SimpleStringString.ProduceMessage(key, value);

        var result = await KafkaConsumers.SimpleStringString.WaitAndGetMessage(
            x => x.Message.Key == key, 5, TimeSpan.FromSeconds(1));

        // Assert
        Assert.That(result, Is.Not.Null);
        Assert.That(result!.Message.Key, Is.EqualTo(key));
    }

    [Test]
    public async Task Consume_string_object_message()
    {
        // Arrange
        var key = GetNewGuid();
        var value = new SimpleClass
        {
            Id = RandomInt(),
            Name = GetNewGuid()
        };

        // Act  
        await KafkaProducers.SimpleStringObject.ProduceMessage(key, value);

        var result = await KafkaConsumers.SimpleStringObject.WaitAndGetMessage(x => x.Message.Key == key);

        // Assert
        Assert.Multiple(() =>
        {
            Assert.That(result, Is.Not.Null);
            Assert.That(result!.Message.Key, Is.EqualTo(key));
            Assert.That(result.Message.Value.Id, Is.EqualTo(value.Id));
            Assert.That(result.Message.Value.Name, Is.EqualTo(value.Name));
        });
    }

    [Test]
    public async Task Consume_string_avro_object_message()
    {
        // Arrange
        var key = GetNewGuid();
        var value = new SimpleAvroClass()
        {
            Id = RandomInt(),
            Name = GetNewGuid()
        };

        //Act
        await KafkaProducers.SimpleStringAvroObject.ProduceMessage(key, value);
        var result = await KafkaConsumers.SimpleStringAvroObject.WaitAndGetMessage(x => x.Message.Key == key);

        // Assert
        Assert.That(result, Is.Not.EqualTo(null!));
        Assert.That(result!.Message.Value.Name, Is.EqualTo(value.Name));
    }

    [Test]
    public async Task Consume_string_protobuf_object_message()
    {
        // Arrange
        var key = GetNewGuid();
        var value = new SimpleProtobufClass()
        {
            Id = RandomInt(),
            Name = GetNewGuid()
        };

        //Act
        await KafkaProducers.SimpleStringProtobufObject.ProduceMessage(key, value);
        var result = await KafkaConsumers.SimpleStringProtobufObject.WaitAndGetMessage(x => x.Message.Key == key);

        // Assert
        Assert.That(result, Is.Not.EqualTo(null!));
        Assert.That(result!.Message.Value.Name, Is.EqualTo(value.Name));
    }

    [Test]
    public async Task Wait_for_no_message()
    {
        // Assert
        await KafkaConsumers.SimpleStringString.WaitForNoMessage(x => x.Message.Key == GetNewGuid(), 3);
    }

    [Test]
    public async Task Wait_for_few_messages()
    {
        // Arrange
        var key = GetNewGuid();
        var value1 = GetNewGuid();
        var value2 = GetNewGuid();

        // Act  
        await KafkaProducers.SimpleStringString.ProduceMessage(key, value1);
        await KafkaProducers.SimpleStringString.ProduceMessage(key, value2);

        var result = await KafkaConsumers.SimpleStringString.WaitAndGetMessages(
            x => x.Message.Key == key,
            result => result.Count == 2);

        // Assert
        Assert.Multiple(() =>
        {
            Assert.That(result, Is.Not.Null);
            Assert.That(result!.Count, Is.EqualTo(2));
            Assert.That(result[0].Message.Value, Is.EqualTo(value1).Or.EqualTo(value2));
            Assert.That(result[1].Message.Value, Is.EqualTo(value1).Or.EqualTo(value2));
            Assert.That(result[1].Message.Value, Is.Not.EqualTo(result[0].Message.Value));
        });
    }

    [Test]
    public async Task Add_filter_to_consumer()
    {
        // Arrange
        var clientsName = "Add_filter_to_consumer";
        var topic = "Add_filter_to_consumer_topic";

        var adminClient = CreateKafkaAdminClient(clientsName);
        await adminClient.CreateTopic(topic);
        var consumer = CreateKafkaConsumer<string, string>(clientsName, topic);
        consumer.Start();
        var producer = CreateKafkaProducer<string, string>(clientsName, topic);

        var keyToSearch = GetNewGuid();
        var keyToDiscard = GetNewGuid();
        var value = GetNewGuid();

        // Act 
        consumer.AddFilter("my-filter", x => x.Message.Key == keyToSearch);
        await producer.ProduceMessage(keyToSearch, value);
        await producer.ProduceMessage(keyToDiscard, value);

        // Assert
        var result = await consumer.WaitAndGetMessage(x => x.Message.Key == keyToSearch);
        Assert.That(result!.Message.Key, Is.EqualTo(keyToSearch));

        result = await consumer.WaitAndGetMessage(x => x.Message.Key == keyToDiscard, 3);
        Assert.That(result, Is.Null);
    }

    [Test]
    public async Task Add_and_remove_filter_from_consumer()
    {
        // Arrange
        var clientsName = "Add_and_remove_filter_from_consumer";
        var topic = "Add_and_remove_filter_from_consumer_topic";

        var adminClient = CreateKafkaAdminClient(clientsName);
        await adminClient.CreateTopic(topic);
        var consumer = CreateKafkaConsumer<string, string>(clientsName, topic);
        consumer.Start();
        var producer = CreateKafkaProducer<string, string>(clientsName, topic);

        var keyToSearch = GetNewGuid();
        var keyToDiscard = GetNewGuid();
        var value = GetNewGuid();

        // Act 
        consumer.AddFilter("my-filter", x => x.Message.Key == keyToSearch);
        await producer.ProduceMessage(keyToSearch, value);
        await producer.ProduceMessage(keyToDiscard, value);

        var result = await consumer.WaitAndGetMessage(x => x.Message.Key == keyToSearch);
        Assert.That(result!.Message.Key, Is.EqualTo(keyToSearch));
        result = await consumer.WaitAndGetMessage(x => x.Message.Key == keyToDiscard, 3);
        Assert.That(result, Is.Null);

        consumer.RemoveFilter("my-filter");
        await producer.ProduceMessage(keyToDiscard, value);

        // Assert
        result = await consumer.WaitAndGetMessage(x => x.Message.Key == keyToDiscard);
        Assert.That(result!.Message.Key, Is.EqualTo(keyToDiscard));
    }

    [Test]
    public async Task Remove_all_saved_messages_by_consumer()
    {
        // Arrange
        var clientsName = "Remove_all_saved_messages_by_consumer";
        var topic = "Remove_all_saved_messages_by_consumer_topic";

        var adminClient = CreateKafkaAdminClient(clientsName);
        await adminClient.CreateTopic(topic);
        var consumer = CreateKafkaConsumer<string, string>(clientsName, topic);
        consumer.Start();
        var producer = CreateKafkaProducer<string, string>(clientsName, topic);

        var key = GetNewGuid();
        var value = GetNewGuid();

        // Act 
        await producer.ProduceMessage(key, value);
        await producer.ProduceMessage(key, value);

        var result = await consumer.WaitAndGetMessages(x => x.Message.Key == key, result => result.Count == 2);
        Assert.That(result!.Count, Is.EqualTo(2));
        consumer.RemoveAllMessages();

        // Assert
        result = await consumer.WaitAndGetMessages(_ => true, retries: 3);
        Assert.That(result, Is.Null);
    }

    [Test]
    public async Task Remove_one_saved_message_by_consumer()
    {
        // Arrange
        var clientsName = "Remove_one_saved_message_by_consumer";
        var topic = "Remove_one_saved_message_by_consumer_topic";

        var adminClient = CreateKafkaAdminClient(clientsName);
        await adminClient.CreateTopic(topic);
        var consumer = CreateKafkaConsumer<string, string>(clientsName, topic);
        consumer.Start();
        var producer = CreateKafkaProducer<string, string>(clientsName, topic);

        var key1 = GetNewGuid();
        var key2 = GetNewGuid();
        var value = GetNewGuid();

        // Act 
        await producer.ProduceMessage(key1, value);
        await producer.ProduceMessage(key2, value);

        var result = await consumer.WaitAndGetMessages(
            x => x.Message.Key == key1 || x.Message.Key == key2,
            result => result.Count == 2);
        Assert.That(result!.Count, Is.EqualTo(2));

        consumer.RemoveMessages(x => x.Message.Key == key2);

        // Assert
        await consumer.WaitForNoMessage(x => x.Message.Key == key2, 3);
        var savedMessage = await consumer.WaitAndGetMessage(x => x.Message.Key == key1);
        Assert.That(savedMessage, Is.Not.Null);
        Assert.That(savedMessage!.Message.Key, Is.EqualTo(key1));
    }

    [Test]
    public async Task Overflow_of_the_buffer_in_consumer()
    {
        // Arrange
        var clientsName = "Overflow_of_the_buffer_in_consumer";
        var topic = "Overflow_of_the_buffer_in_consumer_topic";

        var adminClient = CreateKafkaAdminClient(clientsName);
        await adminClient.CreateTopic(topic);
        var consumer = CreateKafkaConsumer<string, string>(clientsName, topic, 2);
        consumer.Start();
        var producer = CreateKafkaProducer<string, string>(clientsName, topic);

        var key1 = GetNewGuid();
        var key2 = GetNewGuid();
        var key3 = GetNewGuid();
        var value = GetNewGuid();

        // Act 
        await producer.ProduceMessage(key1, value);
        var result = await consumer.WaitAndGetMessage(x => x.Message.Key == key1);
        Assert.That(result!.Message.Key, Is.EqualTo(key1));

        await producer.ProduceMessage(key2, value);
        result = await consumer.WaitAndGetMessage(x => x.Message.Key == key2);
        Assert.That(result!.Message.Key, Is.EqualTo(key2));

        await producer.ProduceMessage(key3, value);

        // Assert
        result = await consumer.WaitAndGetMessage(x => x.Message.Key == key3);
        Assert.That(result!.Message.Key, Is.EqualTo(key3));

        await consumer.WaitForNoMessage(x => x.Message.Key == key1, 3);

        result = await consumer.WaitAndGetMessage(x => x.Message.Key == key2);
        Assert.That(result!.Message.Key, Is.EqualTo(key2));
    }

    [Test]
    public async Task Zero_buffer_size_in_consumer()
    {
        // Arrange
        var clientsName = "Zero_buffer_size_in_consumer";
        var topic = "Zero_buffer_size_in_consumer_topic";

        var adminClient = CreateKafkaAdminClient(clientsName);
        await adminClient.CreateTopic(topic);
        var consumer = CreateKafkaConsumer<string, string>(clientsName, topic, 0);
        consumer.Start();
        var producer = CreateKafkaProducer<string, string>(clientsName, topic);

        var key = GetNewGuid();
        var value = GetNewGuid();

        // Act 
        await producer.ProduceMessage(key, value);

        // Assert
        await consumer.WaitForNoMessage(x => x.Message.Key == key, 3);
    }

    [Test]
    public async Task Consume_string_string_message_and_dispose_consumer()
    {
        // Arrange
        var clientsName = "dispose_consumer";
        var topic = "dispose_consumer_topic";

        var adminClient = CreateKafkaAdminClient(clientsName);
        await adminClient.CreateTopic(topic);

        var consumer = CreateKafkaConsumer<string, string>(clientsName, topic);
        consumer.Start();
        consumer.Start();

        var producer = CreateKafkaProducer<string, string>(clientsName, topic);

        var key = GetNewGuid();
        var value = GetNewGuid();

        // Act 
        await producer.ProduceMessage(key, value);
        var result = await consumer.WaitAndGetMessage(x => x.Message.Key == key);

        Assert.That(result, Is.Not.Null);
        Assert.That(result!.Message.Key, Is.EqualTo(key));

        // Assert
        consumer.Dispose();
        consumer = CreateKafkaConsumer<string, string>(clientsName, topic);
    }

    private KafkaAdminClient CreateKafkaAdminClient(string name)
    {
        return new KafkaAdminClient(name, new Dictionary<string, string>()
        {
            { "bootstrap.servers", "localhost:9092" }
        });
    }

    private KafkaProducer<TKey, TValue> CreateKafkaProducer<TKey, TValue>(string name, string topic)
    {
        return new KafkaProducer<TKey, TValue>(name,
            new ProducerSettings
            {
                Topic = topic,
                Config = new Dictionary<string, string>()
                {
                    { "bootstrap.servers", "localhost:9092" },
                    { "partitioner", "murmur2" },
                    { "acks", "all" },
                    { "message.timeout.ms", "20000" }
                }
            });
    }

    private KafkaConsumer<TKey, TValue> CreateKafkaConsumer<TKey, TValue>(string name, string topic,
        int bufferSize = 5000)
    {
        return new KafkaConsumer<TKey, TValue>(
            name,
            new ConsumerSettings
            {
                Topic = topic,
                BufferSize = bufferSize,
                Config = new Dictionary<string, string>()
                {
                    { "bootstrap.servers", "localhost:9092" },
                    { "auto.offset.reset", "latest" }
                }
            });
    }
}