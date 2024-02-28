using Confluent.Kafka;
using Confluent.Kafka.Admin;
using GR8Tech.TestUtils.KafkaClient.Clients.KafkaAdminClient;
using GR8Tech.TestUtils.KafkaClient.Common.Logging;
using NUnit.Framework;
using Tests.SetUpFixtures;
using ILogger = Serilog.ILogger;

namespace Tests.Tests;

public class AdminClientTests : TestBase
{
    private ILogger _logger = SerilogDecorator.Logger.ForContext<AdminClientTests>();

    [SetUp]
    public void Setup()
    {
    }

    private KafkaAdminClient _localhostAdminClient = new("localhost");
    private KafkaAdminClient _localhostNewAdminClient = new("localhost-new");

    [Test]
    public async Task Create_default_topics()
    {
        // Arrange 
        var topic = "default-topic-to-create";
        var adminClient = _localhostAdminClient;

        // Act
        await adminClient.CreateDefaultTopics();

        // Assert
        Thread.Sleep(2000);
        var desc = await adminClient.AdminClient.DescribeTopicsAsync(TopicCollection.OfTopicNames(new[] { topic }));
        var result = desc.TopicDescriptions.First(x => x.Name == topic);
        Assert.That(result, Is.Not.Null);
        Assert.That(result.Partitions.Count, Is.EqualTo(3));
    }

    [Test]
    public async Task Delete_default_topics()
    {
        // Arrange 
        var topic = "default-topic-to-delete";
        var adminClient = _localhostNewAdminClient;

        // Act
        await adminClient.CreateDefaultTopics();

        var desc = await adminClient.AdminClient.DescribeTopicsAsync(TopicCollection.OfTopicNames(new[] { topic }));
        var result = desc.TopicDescriptions.First(x => x.Name == topic);
        Assert.That(result, Is.Not.Null);

        await adminClient.DeleteDefaultTopics();

        // Assert
        var checker = false;

        try
        {
            await adminClient.AdminClient.DescribeTopicsAsync(TopicCollection.OfTopicNames(new[] { topic }));
        }
        catch (DescribeTopicsException e)
        {
            checker = true;
        }

        Assert.That(checker, Is.True);
    }

    [Test]
    public async Task Create_new_admin_client_from_code()
    {
        // Arrange 
        var name = "new-admin-client-from-code";
        var config = new Dictionary<string, string>
        {
            { "bootstrap.servers", "localhost:9092" }
        };

        // Act
        var adminClient = new KafkaAdminClient(name, config);

        // Assert
        Assert.That(adminClient, Is.Not.Null);
    }

    [Test]
    public async Task Create_new_topic()
    {
        // Arrange 
        var topic = "new-topic-to-create";
        var adminClient = _localhostAdminClient;

        // Act
        await adminClient.CreateTopic(topic, 4);

        // Assert
        var desc = await adminClient.AdminClient.DescribeTopicsAsync(TopicCollection.OfTopicNames(new[] { topic }));
        var result = desc.TopicDescriptions.First(x => x.Name == topic);
        Assert.That(result, Is.Not.Null);
        Assert.That(result.Partitions.Count, Is.EqualTo(4));
    }

    [Test]
    public async Task Delete_new_topic()
    {
        // Arrange 
        var topic = "Delete_new_topic";
        var adminClient = _localhostAdminClient;

        // Act
        await adminClient.CreateTopic(topic);

        var desc = await adminClient.AdminClient.DescribeTopicsAsync(TopicCollection.OfTopicNames(new[] { topic }));
        var result = desc.TopicDescriptions.First(x => x.Name == topic);
        Assert.That(result, Is.Not.Null);

        await adminClient.DeleteTopic(topic);

        // Assert
        var checker = false;

        try
        {
            await adminClient.AdminClient.DescribeTopicsAsync(TopicCollection.OfTopicNames(new[] { topic }));
        }
        catch (DescribeTopicsException e)
        {
            checker = true;
        }

        Assert.That(checker, Is.True);
    }

    [Test]
    public async Task Delete_topics_by_regex()
    {
        // Arrange 
        var topic1 = "regex-1";
        var topic2 = "regex-2";
        var topic3 = "not-match-re-gex";
        var adminClient = _localhostAdminClient;

        // Act
        await adminClient.CreateTopic(topic1);
        await adminClient.CreateTopic(topic2);
        await adminClient.CreateTopic(topic3);

        await adminClient.AdminClient.DescribeTopicsAsync(
            TopicCollection.OfTopicNames(new[] { topic1, topic2, topic3 }));

        await adminClient.DeleteTopicsByRegex(".*regex.*");

        // Assert
        var checker = false;

        try
        {
            await adminClient.AdminClient.DescribeTopicsAsync(TopicCollection.OfTopicNames(new[] { topic1 }));
        }
        catch (DescribeTopicsException e)
        {
            checker = true;
        }

        Assert.That(checker, Is.True);

        checker = false;

        try
        {
            await adminClient.AdminClient.DescribeTopicsAsync(TopicCollection.OfTopicNames(new[] { topic2 }));
        }
        catch (DescribeTopicsException e)
        {
            checker = true;
        }

        Assert.That(checker, Is.True);

        await adminClient.AdminClient.DescribeTopicsAsync(TopicCollection.OfTopicNames(new[] { topic3 }));
    }
}