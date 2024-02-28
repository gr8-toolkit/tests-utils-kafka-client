using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Text.RegularExpressions;
using System.Threading.Tasks;
using Confluent.Kafka;
using Confluent.Kafka.Admin;
using GR8Tech.TestUtils.KafkaClient.Abstractions;
using GR8Tech.TestUtils.KafkaClient.Common;
using GR8Tech.TestUtils.KafkaClient.Common.Logging;
using GR8Tech.TestUtils.KafkaClient.Settings;
using Serilog;

namespace GR8Tech.TestUtils.KafkaClient.Clients.KafkaAdminClient
{
    public class KafkaAdminClient : IKafkaAdminClient
    {
        private static ConcurrentDictionary<string, int> _registeredNames = new ConcurrentDictionary<string, int>();
        private ILogger _logger;
        private List<TopicSettings>? _defaultTopics;

        public IAdminClient AdminClient { get; }
        public string Name { get; }

        public KafkaAdminClient(
            string name) :
            this(name, SettingsProvider.Config.AdminClients![name])
        {
        }

        public KafkaAdminClient(string name, Dictionary<string, string> config)
            : this(name, new AdminClientSettings
            {
                Config = config
            })
        {
        }

        private KafkaAdminClient(
            string name,
            AdminClientSettings settings)
        {
            _logger = Log.Logger
                .ForContext<KafkaAdminClient>()
                .AddPrefix("KafkaAdminClient")
                .AddHiddenPrefix(name);

            if (!_registeredNames.TryAdd(name, 0))
                throw new Exception($"Kafka admin client \"{name}\" already exists");

            if (settings.Config is null)
                throw new Exception($"Kafka admin client \"{name}\" is missing Config for the builder");

            Name = name;
            var config = settings.Config.Where(_ => true).ToDictionary(pair => pair.Key, pair => pair.Value);
            _defaultTopics = settings.Topics;

            if (!config.ContainsKey("bootstrap.servers"))
                throw new Exception($"Kafka admin client \"{name}\" is missing 'bootstrap.servers' in own Config");

            config.TryAdd("security.protocol", "plaintext");

            var adminClientBuilder = new AdminClientBuilder(config);

            AdminClient = adminClientBuilder.Build();
            
            _logger
                .AddPayload("settings", new
                {
                    adminClientName = Name,
                    config
                })
                .Information("New KafkaAdminClient has been built");
        }

        public async Task CreateDefaultTopics()
        {
            if (_defaultTopics != null && _defaultTopics.Count > 0)
            {
                foreach (var topic in _defaultTopics)
                {
                    if (topic.Name == null)
                        throw new Exception("Missing a name for a topic to create");

                    var partitionCount = topic.PartitionCount ?? 1;
                    var replicaCount = topic.ReplicaCount ?? 1;

                    await CreateTopic(topic.Name, partitionCount, replicaCount, topic.TopicConfig!);
                }
            }
        }

        public async Task DeleteDefaultTopics(int retries = 30, int delayMs = 1000)
        {
            if (_defaultTopics != null && _defaultTopics.Count > 0)
            {
                foreach (var topic in _defaultTopics)
                {
                    if (topic.Name == null)
                        throw new Exception("Missing a name for a topic to delete");

                    await DeleteTopic(topic.Name, retries, delayMs);
                }
            }
        }

        public async Task DeleteTopicsByRegex(string pattern, int retries = 30, int delayMs = 1000)
        {
            if (!string.IsNullOrEmpty(pattern))
            {
                Regex regex = new Regex(pattern);

                foreach (var topic in AdminClient.GetMetadata(TimeSpan.FromSeconds(30)).Topics)
                {
                    if (regex.IsMatch(topic.Topic))
                        await DeleteTopic(topic.Topic);
                }
            }
        }

        public async Task DeleteTopic(string name, int retries = 30, int delayMs = 1000)
        {
            var policy = PollyPolicies.AsyncRetryPolicyWithException(
                retries,
                TimeSpan.FromMilliseconds(delayMs));

            try
            {
                await AdminClient.DeleteTopicsAsync(new[] { name });

                await policy.ExecuteAsync(async () =>
                    {
                        try
                        {
                            await AdminClient.DescribeTopicsAsync(TopicCollection.OfTopicNames(new[] { name }));
                        }
                        catch (DescribeTopicsException e)
                        {
                            _logger.Information("Topic \"{name}\" has been deleted successfully", name);

                            return;
                        }

                        throw new Exception($"Waiting for the topic \"{name}\" to be deleted");
                    }
                );
            }
            catch (DeleteTopicsException e)
            {
                foreach (var result in e.Results)
                {
                    if (result.Error.Code == ErrorCode.NoError ||
                        result.Error.Code == ErrorCode.Local_UnknownTopic)
                    {
                        _logger.Warning("Was no able to delete the topic. Reason: {reason}",
                            result.Error.Reason);

                        continue;
                    }

                    _logger.Error("Failed to delete the topic. Reason: {reason}", result.Error.Reason);

                    throw;
                }
            }
        }

        public async Task CreateTopic(
            string name,
            int partitionCount = 1,
            short replicaCount = 1,
            Dictionary<string, string> topicConfig = null!)
        {
            var specification = new TopicSpecification
            {
                Configs = topicConfig,
                Name = name,
                NumPartitions = partitionCount,
                ReplicationFactor = replicaCount
            };

            try
            {
                await AdminClient.CreateTopicsAsync(new[] { specification });

                _logger
                    .AddPayload("specification", specification)
                    .Information("Topic \"{name}\" has been created successfully", name);
            }
            catch (CreateTopicsException e)
            {
                foreach (var result in e.Results)
                {
                    if (result.Error.Code == ErrorCode.NoError || result.Error.Code == ErrorCode.TopicAlreadyExists)
                    {
                        _logger.Warning("Was no able to create the topic. Reason: {reason}", result.Error.Reason);

                        continue;
                    }

                    _logger.Error("Failed to create the topic. Reason: {reason}", result.Error.Reason);

                    throw;
                }
            }
        }

        public void Dispose()
        {
            AdminClient.Dispose();
            
            if (!_registeredNames.TryRemove(Name, out _))
                _logger.Warning("Failed to remove the admin client from the registered list");
        }
    }
}