using System;
using System.Text;
using Confluent.Kafka;
using Newtonsoft.Json;
using Newtonsoft.Json.Converters;

namespace GR8Tech.TestUtils.KafkaClient.Common
{
    public class UTF8JsonDeserializer<T> : IDeserializer<T>
    {
        private bool _stringEnumConverter;

        public UTF8JsonDeserializer(bool isStringEnumConverter = false)
        {
            _stringEnumConverter = isStringEnumConverter;
        }

        public T Deserialize(ReadOnlySpan<byte> data, bool isNull, SerializationContext context)
        {
            if (isNull) return default;

            var settings = new JsonSerializerSettings
            {
                Formatting = Formatting.None,
            };

            if (_stringEnumConverter)
                settings.Converters.Add(new StringEnumConverter());

            var s = Encoding.UTF8.GetString(data);

            return JsonConvert.DeserializeObject<T>(s, settings);
        }
    }
}