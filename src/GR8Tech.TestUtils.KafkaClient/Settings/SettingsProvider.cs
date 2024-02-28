using System.Collections.Generic;
using System.IO;
using GR8Tech.TestUtils.KafkaClient.Common.Logging;
using Microsoft.Extensions.Configuration;
using Newtonsoft.Json;
using Serilog;
using static System.Environment; 

namespace GR8Tech.TestUtils.KafkaClient.Settings
{
    internal static class SettingsProvider
    {
        static SettingsProvider()
        {
            var configFileName = File.Exists("test-settings.json") ? "test-settings" : "appsettings";
            File.WriteAllText("kafka-settings.json", File.ReadAllText($"{configFileName}.json"));

            var env = GetEnvironmentVariable("ASPNETCORE_ENVIRONMENT");

            if (File.Exists($"{configFileName}.{env}.json"))
                File.WriteAllText($"kafka-settings.{env}.json", File.ReadAllText($"{configFileName}.{env}.json"));

            var tempConfig = ReadConfig(configFileName);

            if (tempConfig.TemplateVariables == null || tempConfig.TemplateVariables.Count == 0)
                Config = tempConfig;
            else
            {
                foreach (var variable in tempConfig.TemplateVariables)
                {
                    var basicSettings = File.ReadAllText("kafka-settings.json");
                    basicSettings = basicSettings.Replace("${" + variable.Key + "}", variable.Value);
                    File.WriteAllText("kafka-settings.json", basicSettings);

                    if (File.Exists($"kafka-settings.{env}.json"))
                    {
                        var envSettings = File.ReadAllText($"kafka-settings.{env}.json");
                        envSettings = envSettings.Replace("${" + variable.Key + "}", variable.Value);
                        File.WriteAllText($"kafka-settings.{env}.json", envSettings);
                    }
                }

                Config = ReadConfig("kafka-settings");
            }

            Log.Logger
                .ForContextStaticClass(typeof(SettingsProvider))
                .AddPayload("KafkaSettings", JsonConvert.SerializeObject(Config, Formatting.Indented))
                .Information("KafkaSettings have been read and initialized");
        }

        public static KafkaSettings Config { get; }

        private static KafkaSettings ReadConfig(string configFileName)
        {
            return new ConfigurationBuilder()
                .SetBasePath(Directory.GetCurrentDirectory())
                .AddJsonFile($"{configFileName}.json", false, true)
                .AddJsonFile($"{configFileName}.{GetEnvironmentVariable("ASPNETCORE_ENVIRONMENT")}.json", true)
                .AddEnvironmentVariables()
                .Build()
                .GetSection("KafkaSettings")
                .Get<KafkaSettings>()!
                .SetCascadeSettings();
        }

        private static KafkaSettings SetCascadeSettings(this KafkaSettings settings)
        {
            var defaultConsumerSettings = settings.DefaultConsumerSettings ?? new ConsumerSettings();
            defaultConsumerSettings.WaiterSettings ??= new WaiterSettings();
            defaultConsumerSettings.Topic = null;
            defaultConsumerSettings.IndependentConfig ??= false;
            defaultConsumerSettings.Config ??= settings.DefaultClientConfig;
            defaultConsumerSettings.Config?.CascadeDictionaryUpdate(settings.DefaultClientConfig!);

            var defaultProducerSettings = settings.DefaultProducerSettings ?? new ProducerSettings();
            defaultProducerSettings.Topic = null;
            defaultProducerSettings.IndependentConfig ??= false;
            defaultProducerSettings.Config ??= settings.DefaultClientConfig;
            defaultProducerSettings.Config?.CascadeDictionaryUpdate(settings.DefaultClientConfig!);

            if (settings.Consumers != null)
            {
                foreach (var consumer in settings.Consumers)
                {
                    consumer.Value.BufferSize ??= defaultConsumerSettings.BufferSize;
                    consumer.Value.WaiterSettings ??= defaultConsumerSettings.WaiterSettings;
                    consumer.Value.WaiterSettings.RetryCount ??= defaultConsumerSettings.WaiterSettings.RetryCount;
                    consumer.Value.WaiterSettings.Interval ??= defaultConsumerSettings.WaiterSettings.Interval;
                    consumer.Value.SubscribeOverAssign ??= defaultConsumerSettings.SubscribeOverAssign;
                    consumer.Value.IndependentConfig ??= defaultConsumerSettings.IndependentConfig;

                    if ((bool)consumer.Value.IndependentConfig) continue;

                    consumer.Value.Config ??= defaultConsumerSettings.Config;
                    consumer.Value.Config?.CascadeDictionaryUpdate(defaultConsumerSettings.Config!);
                }
            }

            if (settings.Producers != null)
            {
                foreach (var producer in settings.Producers)
                {
                    producer.Value.IndependentConfig ??= defaultProducerSettings.IndependentConfig;

                    if ((bool)producer.Value.IndependentConfig) continue;

                    producer.Value.Config ??= defaultProducerSettings.Config;
                    producer.Value.Config?.CascadeDictionaryUpdate(defaultProducerSettings.Config!);
                }
            }

            if (settings.AdminClients != null)
            {
                foreach (var adminClient in settings.AdminClients)
                {
                    adminClient.Value.IndependentConfig ??= false;
                    adminClient.Value.DefaultTopicSettings ??= new TopicSettings();
                    adminClient.Value.DefaultTopicSettings.IndependentTopicConfig ??= false;
                    adminClient.Value.DefaultTopicSettings.Name = null;

                    if (adminClient.Value.Topics != null)
                    {
                        foreach (var topic in adminClient.Value.Topics)
                        {
                            topic.PartitionCount ??= adminClient.Value.DefaultTopicSettings.PartitionCount;
                            topic.ReplicaCount ??= adminClient.Value.DefaultTopicSettings.ReplicaCount;
                            topic.IndependentTopicConfig ??=
                                adminClient.Value.DefaultTopicSettings.IndependentTopicConfig;

                            if ((bool)topic.IndependentTopicConfig) continue;

                            topic.TopicConfig ??= adminClient.Value.DefaultTopicSettings.TopicConfig;
                            topic.TopicConfig?.CascadeDictionaryUpdate(adminClient.Value.DefaultTopicSettings
                                .TopicConfig!);
                        }
                    }

                    if ((bool)adminClient.Value.IndependentConfig) continue;

                    adminClient.Value.Config ??= settings.DefaultClientConfig;
                    adminClient.Value.Config?.CascadeDictionaryUpdate(settings.DefaultClientConfig!);
                }
            }

            return settings;
        }

        private static void CascadeDictionaryUpdate<TKey, TValue>(this Dictionary<TKey, TValue> targetDictionary,
            Dictionary<TKey, TValue> originalDictionary)
        {
            if (originalDictionary == null! || targetDictionary == originalDictionary) return;

            foreach (var pair in originalDictionary)
                targetDictionary.TryAdd(pair.Key, pair.Value);
        }
    }
}