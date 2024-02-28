using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Confluent.Kafka;
using GR8Tech.TestUtils.KafkaClient.Abstractions;
using GR8Tech.TestUtils.KafkaClient.Common;
using GR8Tech.TestUtils.KafkaClient.Common.Logging;
using GR8Tech.TestUtils.KafkaClient.Settings;
using Serilog;

namespace GR8Tech.TestUtils.KafkaClient.Clients.KafkaProducer
{
    public class KafkaProducer<TKey, TValue> : IKafkaProducer<TKey, TValue>
    {
        private ILogger _logger;
        private static ConcurrentDictionary<string, int> _registeredNames = new ConcurrentDictionary<string, int>();
        public IProducer<TKey, TValue> Producer { get; }
        public string TopicName { get; }
        public string Name { get; }

        public KafkaProducer(
            string name,
            ISerializer<TKey>? keySerializer = null,
            ISerializer<TValue>? valueSerializer = null) :
            this(
                name,
                SettingsProvider.Config.Producers![name],
                keySerializer,
                valueSerializer)
        {
        }

        public KafkaProducer(
            string name,
            ProducerSettings settings,
            ISerializer<TKey>? keySerializer = null,
            ISerializer<TValue>? valueSerializer = null)
        {
            _logger = Log.Logger
                .ForContext<KafkaProducer<TKey, TValue>>()
                .AddPrefix("KafkaProducer")
                .AddHiddenPrefix(name);

            if (!_registeredNames.TryAdd(name, 0))
                throw new Exception($"Kafka producer \"{name}\" already exists");

            if (settings.Config is null)
                throw new Exception($"Kafka producer[{name}] is missing Config for the builder");

            TopicName = settings.Topic ?? throw new Exception($"Kafka producer[{name}] is missing topic");
            Name = name;
            var config = settings.Config.Where(_ => true).ToDictionary(pair => pair.Key, pair => pair.Value);

            if (!config.ContainsKey("bootstrap.servers"))
                throw new Exception($"Kafka producer[{name}] is missing 'bootstrap.servers' in own Config");

            config.TryAdd("message.max.bytes", "200000000");
            config.TryAdd("partitioner", "murmur2");
            config.TryAdd("acks", "all");
            config.TryAdd("message.timeout.ms", "20000");
            config.TryAdd("security.protocol", "plaintext");

            var producerBuilder = new ProducerBuilder<TKey, TValue>(config);

            if (keySerializer == null && typeof(TKey) != typeof(string))
                producerBuilder.SetKeySerializer(new UTF8JsonSerializer<TKey>());

            if (keySerializer != null)
                producerBuilder.SetKeySerializer(keySerializer);

            if (valueSerializer == null && typeof(TValue) != typeof(string))
                producerBuilder.SetValueSerializer(new UTF8JsonSerializer<TValue>());

            if (valueSerializer != null)
                producerBuilder.SetValueSerializer(valueSerializer);

            Producer = producerBuilder.Build();

            _logger
                .AddPayload("settings", new
                {
                    producerName = Name,
                    topicName = TopicName,
                    config
                })
                .Information("New KafkaProducer has been built");
        }

        public async Task<DeliveryResult<TKey, TValue>> ProduceMessage(Message<TKey, TValue> message)
        {
            DeliveryResult<TKey, TValue> deliveryResult = null;

            try
            {
                deliveryResult = await Producer.ProduceAsync(TopicName, message);

                _logger
                    .AddPayload("message", deliveryResult.Value)
                    .Information("Message was produced, offset \"{offset}\", key \"{key}\"",
                        deliveryResult.Offset.Value, deliveryResult.Message.Key);

                return deliveryResult;
            }
            catch (ProduceException<TKey, TValue> e)
            {
                _logger
                    .AddPayload("message", message.Value)
                    .Error(e, "Failed to produce the message, key \"{key}\"", message.Key);
            }

            return deliveryResult!;
        }

        public async Task<DeliveryResult<TKey, TValue>> ProduceMessage(TKey key, TValue message)
        {
            return await ProduceMessage(new Message<TKey, TValue>
            {
                Key = key,
                Value = message
            });
        }

        public async Task<DeliveryResult<TKey, TValue>> ProduceMessage(TKey key, TValue message, Headers headers)
        {
            return await ProduceMessage(new Message<TKey, TValue>
            {
                Key = key,
                Value = message,
                Headers = headers
            });
        }

        public async Task<DeliveryResult<TKey, TValue>> ProduceMessage(
            TKey key,
            TValue message,
            Dictionary<string, string> headers)
        {
            var formatedHeaders = new Headers();

            foreach (var header in headers)
                formatedHeaders.Add(header.Key, Encoding.UTF8.GetBytes(header.Value));

            return await ProduceMessage(new Message<TKey, TValue>
            {
                Key = key,
                Value = message,
                Headers = formatedHeaders
            });
        }

        public void Dispose()
        {
            Producer.Dispose();
            
            if(!_registeredNames.TryRemove(Name, out _))
                _logger.Warning("Failed to remove the producer from the registered list");
        }
    }
}