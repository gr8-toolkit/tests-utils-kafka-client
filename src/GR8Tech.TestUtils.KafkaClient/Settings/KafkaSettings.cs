using System;
using System.Collections.Generic;

namespace GR8Tech.TestUtils.KafkaClient.Settings
{
    internal sealed class KafkaSettings
    {
        public Dictionary<string, string>? TemplateVariables { get; set; }
        public Dictionary<string, string>? DefaultClientConfig { get; set; }
        public ConsumerSettings? DefaultConsumerSettings { get; set; }
        public ProducerSettings? DefaultProducerSettings { get; set; }
        public Dictionary<string, AdminClientSettings>? AdminClients { get; set; }
        public Dictionary<string, ConsumerSettings>? Consumers { get; set; }
        public Dictionary<string, ProducerSettings>? Producers { get; set; }
    }
    
    public class AdminClientSettings
    {
        public bool? IndependentConfig { get; set; }
        public Dictionary<string, string>? Config { get; set; }
        public TopicSettings? DefaultTopicSettings { get; set; }
        public List<TopicSettings>? Topics { get; set; }
    }
    
    public class TopicSettings
    {
        public string? Name { get; set; }
        public int? PartitionCount { get; set; }
        public short? ReplicaCount { get; set; }
        public bool? IndependentTopicConfig { get; set; }
        public Dictionary<string, string>? TopicConfig { get; set; }
    }


    public class ConsumerSettings
    {
        public string? Topic { get; set; }
        public int? BufferSize { get; set; }
        public WaiterSettings? WaiterSettings { get; set; }
        public bool? SubscribeOverAssign { get; set; }
        public bool? IndependentConfig { get; set; }
        public Dictionary<string, string>? Config { get; set; }
    }
    
    public class ProducerSettings
    {
        public string? Topic { get; set; }
        public bool? IndependentConfig { get; set; }
        public Dictionary<string, string>? Config { get; set; }
    }
    
    public class WaiterSettings
    {
        public int? RetryCount { get; set; }
        public TimeSpan? Interval { get; set; }
    }
}