using System.Text;
using Confluent.Kafka;
using Newtonsoft.Json;
using Newtonsoft.Json.Converters;

namespace GR8Tech.TestUtils.KafkaClient.Common
{
    public class UTF8JsonSerializer<T> : ISerializer<T>
    {
        private bool _stringEnumConverter;

        public UTF8JsonSerializer(bool isStringEnumConverter = false)
        {
            _stringEnumConverter = isStringEnumConverter;
        }

        public byte[] Serialize(T data, SerializationContext context)
        {
            var settings = new JsonSerializerSettings
            {
                Formatting = Formatting.None,
            };

            if (_stringEnumConverter)
                settings.Converters.Add(new StringEnumConverter());

            return Encoding.UTF8.GetBytes(JsonConvert.SerializeObject(data, settings));
        }
    }
}

