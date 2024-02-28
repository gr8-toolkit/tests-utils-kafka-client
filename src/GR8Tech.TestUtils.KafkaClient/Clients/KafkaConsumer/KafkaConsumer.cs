using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Confluent.Kafka;
using Confluent.Kafka.Admin;
using GR8Tech.TestUtils.KafkaClient.Abstractions;
using GR8Tech.TestUtils.KafkaClient.Common;
using GR8Tech.TestUtils.KafkaClient.Common.Logging;
using GR8Tech.TestUtils.KafkaClient.Settings;
using Newtonsoft.Json;
using Serilog;

namespace GR8Tech.TestUtils.KafkaClient.Clients.KafkaConsumer
{
    public class KafkaConsumer<TKey, TValue> : IKafkaConsumer<TKey, TValue>
    {
        private ILogger _logger;
        private string _offset;
        private int _bufferSize;
        private int _waiterRetries;
        private TimeSpan _waiterInterval; 
        private bool _subscribeOverAssign;
        private Dictionary<string, string> _adminClientConfig;
        private CancellationTokenSource _cancellationTokenSourceForConsuming;
        private Task _consumingTask;

        private readonly ConcurrentDictionary<string, Func<ConsumeResult<TKey, TValue>, bool>> _filters =
            new ConcurrentDictionary<string, Func<ConsumeResult<TKey, TValue>, bool>>();

        private ConcurrentDictionary<int, ConsumeResult<TKey, TValue>> _messages =
            new ConcurrentDictionary<int, ConsumeResult<TKey, TValue>>();

        private static ConcurrentDictionary<string, int> _registeredNames = new ConcurrentDictionary<string, int>();

        public IConsumer<TKey, TValue> Consumer { get; }
        public string TopicName { get; }
        public string Name { get; }

        public KafkaConsumer(
            string name,
            IDeserializer<TKey>? keyDeserializer = null,
            IDeserializer<TValue>? valueDeserializer = null) :
            this(
                name,
                SettingsProvider.Config.Consumers![name],
                keyDeserializer,
                valueDeserializer)
        {
        }

        public KafkaConsumer(
            string name,
            ConsumerSettings settings,
            IDeserializer<TKey>? keyDeserializer = null,
            IDeserializer<TValue>? valueDeserializer = null)
        {
            _logger = Log.Logger
                .ForContext<KafkaConsumer<TKey, TValue>>()
                .AddPrefix("KafkaConsumer")
                .AddHiddenPrefix(name);

            if (!_registeredNames.TryAdd(name, 0))
                throw new Exception($"Kafka consumer \"{name}\" already exists");

            if (settings.Config is null)
                throw new Exception($"Kafka consumer \"{name}\" is missing Config for the builder");

            TopicName = settings.Topic ?? throw new Exception($"Kafka consumer \"{name}\" is missing topic");
            Name = name;
            _bufferSize = settings.BufferSize ?? 5000;
            _waiterRetries = settings.WaiterSettings?.RetryCount ?? 30;
            _waiterInterval = settings.WaiterSettings?.Interval ?? TimeSpan.FromSeconds(1);
            _subscribeOverAssign = settings.SubscribeOverAssign ?? false;
            var config = settings.Config.Where(_ => true).ToDictionary(pair => pair.Key, pair => pair.Value);

            if (!config.ContainsKey("bootstrap.servers"))
                throw new Exception($"Kafka consumer[{name}] is missing 'bootstrap.servers' in own Config");

            config.TryAdd("group.id", Guid.NewGuid().ToString("N"));
            config.TryAdd("auto.offset.reset", "latest");
            config.TryAdd("message.max.bytes", "200000000");
            config.TryAdd("security.protocol", "plaintext");

            _offset = config["auto.offset.reset"];
            _adminClientConfig = config.Where(x =>
                    x.Key.Contains("security.protocol") ||
                    x.Key.Contains("ssl.") ||
                    x.Key.Contains("bootstrap.servers"))
                .ToDictionary(pair => pair.Key, pair => pair.Value);

            _adminClientConfig.TryAdd("allow.auto.create.topics", "true");

            var consumerBuilder = new ConsumerBuilder<TKey, TValue>(config);

            if (keyDeserializer == null && typeof(TKey) != typeof(string))
                consumerBuilder.SetKeyDeserializer(new UTF8JsonDeserializer<TKey>());

            if (keyDeserializer != null)
                consumerBuilder.SetKeyDeserializer(keyDeserializer);

            if (valueDeserializer == null && typeof(TValue) != typeof(string))
                consumerBuilder.SetValueDeserializer(new UTF8JsonDeserializer<TValue>());

            if (valueDeserializer != null)
                consumerBuilder.SetValueDeserializer(valueDeserializer);

            Consumer = consumerBuilder.Build();

            _filters.TryAdd("default", _ => true);

            _logger
                .AddPayload("settings", new
                {
                    consumerName = Name,
                    topicName = TopicName,
                    bufferSize = _bufferSize,
                    waiterRetries = _waiterRetries,
                    waiterInterval = _waiterInterval,
                    subscribeOverAssign = _subscribeOverAssign,
                    config,
                    adminClientConfig = _adminClientConfig
                })
                .Information("New KafkaConsumer has been built");
        }

        private int _startControl = 0;

        public void Start()
        {
            if (Interlocked.Increment(ref _startControl) > 1)
            {
                _logger.Warning("Start has already been called");
                return;
            }
            
            var topicPartitionOffsets = GetTopicPartitionOffsets().Result.ToArray();

            if (_subscribeOverAssign)
                Consumer.Subscribe(TopicName);
            else
                Consumer.Assign(topicPartitionOffsets);

            WaitForSubscription(topicPartitionOffsets.Length);

            var firstResult = Consumer.Consume(TimeSpan.FromSeconds(1));

            if (firstResult != null && _bufferSize > 0)
                _messages.TryAdd(GetIndex(), firstResult);

            _logger.Information("KafkaConsumer is ready to consume");

            _cancellationTokenSourceForConsuming = new CancellationTokenSource();
            _consumingTask = Task.Run(ConsumeMessages);
        }

        public void AddFilter(string key, Func<ConsumeResult<TKey, TValue>, bool> filter)
        {
            if (_filters.ContainsKey("default"))
            {
                if (_filters.TryRemove("default", out _))
                    _logger.Information("Default filter has been deleted successfully");
                else
                    _logger.Warning(
                        "Default filter was NOT deleted successfully. It might have been deleted by another thread");
            }

            _filters.AddOrUpdate(key, filter, (name, oldFilter) => filter);

            _logger.Information("Filter \"{filter}\" has been added successfully", key);
        }

        public void RemoveFilter(string key)
        {
            if (!_filters.ContainsKey(key)) return;

            if (_filters.TryRemove(key, out _))
                _logger.Information("Filter \"{name}\" has been deleted successfully", key);
            else
                _logger.Warning(
                    "Filter \"{name}\" has NOT been deleted successfully. It might have been deleted by another thread",
                    key);

            if (_filters.Count == 0)
                _filters.TryAdd("default", _ => true);
        }

        public async Task WaitForNoMessage(
            Func<ConsumeResult<TKey, TValue>, bool> compare, int? retries = null, TimeSpan? interval = null)
        {
            var messages = (await WaitAndGetMessages(compare, null, retries, interval))?.ToArray();

            if (messages != null && messages.Any())
            {
                var keys = messages.Select(x => x.Message.Key);

                throw new Exception(
                    $"{messages.Length} messages were found in kafka topic[{TopicName}] when should NOT have been. " +
                    $"Keys: \n{JsonConvert.SerializeObject(keys)}");
            }
        }

        public async Task<ConsumeResult<TKey, TValue>?> WaitAndGetMessage(
            Func<ConsumeResult<TKey, TValue>, bool> compare, int? retries = null, TimeSpan? interval = null)
        {
            var messages = await WaitAndGetMessages(compare, null, retries, interval);

            if (messages != null && messages.Any())
            {
                if (messages.Count > 1)
                    _logger.Warning("Be aware that more than one message found for your condition");

                return messages.First();
            }

            return null;
        }

        public async Task<List<ConsumeResult<TKey, TValue>>?> WaitAndGetMessages(
            Func<ConsumeResult<TKey, TValue>, bool> compare,
            Func<List<ConsumeResult<TKey, TValue>>, bool>? condition = null,
            int? retries = null,
            TimeSpan? interval = null)
        {
            retries ??= _waiterRetries;
            interval ??= _waiterInterval;

            if (condition == null)
                condition = result => true;

            var policy = PollyPolicies.AsyncRetryPolicyWithExceptionAndResult<List<ConsumeResult<TKey, TValue>>>(
                (int)retries, (TimeSpan)interval);

            return await policy.ExecuteAsync(async () =>
                {
                    var result = _messages.Values.Where(compare).ToList();

                    return (result.Any() && condition(result) ? result : default)!;
                }
            );
        }

        public void RemoveAllMessages()
        {
            _messages.Clear();
            _logger.Information("All messages have been deleted successfully from the buffer");
        }

        public void RemoveMessages(Func<ConsumeResult<TKey, TValue>, bool> compare)
        {
            var messagesToRemove = _messages.Where(x => compare(x.Value));

            foreach (var message in messagesToRemove)
                if (!_messages.TryRemove(message.Key, out _))
                    _logger.Warning("Failed to delete message with key: {key}", message.Value.Message.Key);
        }

        public void Dispose()
        {
            _cancellationTokenSourceForConsuming.Cancel();
            _consumingTask.Wait();

            Consumer.Close();
            Consumer.Dispose();

            if (!_registeredNames.TryRemove(Name, out _))
                _logger.Warning("Failed to remove the consumer from the registered list");
        }

        private void WaitForSubscription(int partitionCount)
        {
            var policy = PollyPolicies.AsyncRetryPolicyWithException(120, TimeSpan.FromSeconds(1));

            policy.ExecuteAsync(() =>
            {
                if (Consumer.Assignment.Count != partitionCount && string.IsNullOrEmpty(Consumer.MemberId))
                    throw new Exception($"Timeout for consumer \"{Name}\" subscription");

                return Task.CompletedTask;
            }).Wait();
        }

        private int _bufferCounter;

        private int GetIndex()
        {
            Interlocked.CompareExchange(ref _bufferCounter, 0, _bufferSize);
            return Interlocked.Add(ref _bufferCounter, 1);
        }

        private void ConsumeMessages()
        {
            while (!_cancellationTokenSourceForConsuming.Token.IsCancellationRequested)
            {
                var consumeResult = new ConsumeResult<TKey, TValue>();

                try
                {
                    consumeResult = Consumer.Consume(TimeSpan.FromSeconds(2));
                }
                catch (ConsumeException e)
                {
                    _logger.Error(e, "Failed to consume the message. Reason {reason}", e.Error.Reason);
                }

                if (consumeResult is null) continue;

                if (_filters.Any(filter => filter.Value(consumeResult)) && _bufferSize > 0)
                {
                    _messages.AddOrUpdate(GetIndex(), consumeResult, (oldIndex, oldMessage) => consumeResult);

                    _logger
                        .AddPayload("message", consumeResult.Message.Value)
                        .Information("Received a new message, offset \"{offset}\", key \"{key}\"",
                            consumeResult.Offset.Value, consumeResult.Message.Key);

                    continue;
                }

                _logger.Information(
                    "Received but filtered out a new message, offset \"{offset}\", key \"{key}\" ",
                    consumeResult.Offset.Value, consumeResult.Message.Key);
            }
        }

        private async Task<IEnumerable<TopicPartitionOffset>> GetTopicPartitionOffsets()
        {
            DescribeTopicsResult topicsDescription;

            using (var adminClient = new AdminClientBuilder(_adminClientConfig).Build())
            {
                try
                {
                    topicsDescription =
                        await adminClient.DescribeTopicsAsync(TopicCollection.OfTopicNames(new[] { TopicName }));
                }
                catch (DescribeTopicsException e)
                {
                    _logger.Warning(e, "Failed to get topic description for topic: {TopicName}", TopicName);

                    await adminClient.CreateTopicsAsync(new[]
                    {
                        new TopicSpecification
                        {
                            Name = TopicName,
                            NumPartitions = 1,
                            ReplicationFactor = 1
                        }
                    });

                    _logger.Information("Topic {TopicName} was auto created with default settings", TopicName);

                    topicsDescription =
                        await adminClient.DescribeTopicsAsync(TopicCollection.OfTopicNames(new[] { TopicName }));
                }
            }

            var topicPartitions = topicsDescription?.TopicDescriptions.First(x => x.Name == TopicName).Partitions;

            if (topicPartitions is null || topicPartitions.Count == 0)
                throw new Exception($"Error while getting topic partitions for {TopicName} by admin client");

            var offset = Offset.Beginning;

            if (_offset == "latest" || _offset == "largest")
                offset = Offset.End;

            var topicPartitionOffsets = new List<TopicPartitionOffset>();
            topicPartitionOffsets.AddRange(topicPartitions.Select(partition =>
                new TopicPartitionOffset(TopicName, new Partition(partition.Partition), offset)));

            return topicPartitionOffsets;
        }
    }
}