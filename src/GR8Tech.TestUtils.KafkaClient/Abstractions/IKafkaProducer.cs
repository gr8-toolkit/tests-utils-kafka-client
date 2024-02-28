using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Confluent.Kafka;

namespace GR8Tech.TestUtils.KafkaClient.Abstractions
{
    public interface IKafkaProducer<TKey, TValue> : IDisposable
    {
        /// <summary>
        /// An access to built Kafka Producer
        /// </summary>
        IProducer<TKey, TValue> Producer { get; }
        
        /// <summary>
        /// Topic name KafkaProducer is producing to
        /// </summary>
        string TopicName { get; }
        
        /// <summary>
        /// A name of created KafkaProducer. Must be unique
        /// </summary>
        string Name { get; }
        
        /// <summary>
        /// Produces new Message&lt;TKey, TValue&gt; to kafka topic
        /// </summary>
        /// <param name="message">Message to produce</param>
        /// <returns>Returns delivery result</returns>
        Task<DeliveryResult<TKey, TValue>> ProduceMessage(Message<TKey, TValue> message);
        
        /// <summary>
        /// Produces new message to kafka topic with defined key and value
        /// </summary>
        /// <param name="key">Message key</param>
        /// <param name="message">Message body (value)</param>
        /// <returns>Returns delivery result</returns>
        Task<DeliveryResult<TKey, TValue>> ProduceMessage(TKey key, TValue message);
        
        /// <summary>
        /// Produces new message to kafka topic with defined key, value and headers
        /// </summary>
        /// <param name="key">Message key</param>
        /// <param name="message">Message body (value)</param>
        /// <param name="headers">Message headers</param>
        /// <returns>Returns delivery result</returns>
        Task<DeliveryResult<TKey, TValue>> ProduceMessage(TKey key, TValue message, Headers headers);

        /// <summary>
        /// Produces new message to kafka topic with defined key, value and headers
        /// </summary>
        /// <param name="key">Message key</param>
        /// <param name="message">Message body (value)</param>
        /// <param name="headers">Message headers</param>
        /// <returns>Returns delivery result</returns>
        Task<DeliveryResult<TKey, TValue>> ProduceMessage(
            TKey key,
            TValue message,
            Dictionary<string, string> headers);
    }
}