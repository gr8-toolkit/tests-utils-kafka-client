using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Confluent.Kafka;

namespace GR8Tech.TestUtils.KafkaClient.Abstractions
{
    /// <summary>
    /// KafkaAdminClient helps to communicate and interact with Kafka brokers
    /// </summary>
    public interface IKafkaAdminClient : IDisposable
    {
        /// <summary>
        /// An access to built Kafka AdminClient
        /// </summary>
        IAdminClient AdminClient { get; }

        /// <summary>
        /// A name of created KafkaAdminClient. Must be unique
        /// </summary>
        string Name { get; }

        /// <summary>
        /// Creates topics defined in a settings json file
        /// </summary>
        Task CreateDefaultTopics();

        /// <summary>
        /// Deletes topics defined in a settings json file. Insures that topics have been deleted with some timeout
        /// </summary>
        /// <param name="retries">How many retries to do to make sure that topic has been deleted. Default: 30</param>
        /// <param name="delayMs">Delay in ms between retries. Default: 1000 ms</param>
        Task DeleteDefaultTopics(int retries = 30, int delayMs = 1000);

        /// <summary>
        /// Deletes topics by Regex pattern. Insures that topics have been deleted with some timeout
        /// </summary>
        /// <param name="pattern">Regex pattern. Example ".*local.*"</param>
        /// <param name="retries">How many retries to do to make sure that topic has been deleted. Default: 30</param>
        /// <param name="delayMs">Delay in ms between retries. Default: 1000 ms</param>
        Task DeleteTopicsByRegex(string pattern, int retries = 30, int delayMs = 1000);

        /// <summary>
        /// Deletes single topic. Insures that topic have been deleted with some timeout
        /// </summary>
        /// <param name="name">Topic name to delete</param>
        /// <param name="retries">How many retries to do to make sure that topic has been deleted. Default: 30</param>
        /// <param name="delayMs">Delay in ms between retries. Default: 1000 ms</param>
        Task DeleteTopic(string name, int retries = 30, int delayMs = 1000);

        /// <summary>
        /// Create single topic
        /// </summary>
        /// <param name="name">Topic name to create</param>
        /// <param name="partitionCount">How many partitions topic should have. Default: 1</param>
        /// <param name="replicaCount">How many replicas each topic partition should have. Default: 1</param>
        /// <param name="topicConfig">Topic configuration</param>
        Task CreateTopic(
            string name,
            int partitionCount = 1,
            short replicaCount = 1,
            Dictionary<string, string> topicConfig = null!);
    }
}