using System;
using Newtonsoft.Json;
using Serilog;
using Serilog.Core;

namespace GR8Tech.TestUtils.KafkaClient.Common.Logging
{
    public static class SerilogExtensions
    {
        /// <summary>
        /// <p>Add a new property with a name "prefix" to the context</p>
        /// <p>Note: Prefix is visible in the log message regardless of the log level</p>
        /// </summary>
        /// <param name="logger">Current instance of Serilog</param>
        /// <param name="prefix">String value to save as a prefix</param>
        /// <returns>New instance of Serilog with new context</returns>
        public static ILogger AddPrefix(this ILogger logger, string prefix)
        {
            return logger
                .ForContext("prefix", prefix);
        }

        /// <summary>
        /// <p>Add a new property with a name "hidden_prefix" to the context</p>
        /// <p>Note: HiddenPrefix is visible in the log message only if the log level is Verbose or Debug</p>
        /// </summary>
        /// <param name="logger">Current instance of Serilog</param>
        /// <param name="hiddenPrefix">String value to save as a hidden prefix</param>
        /// <returns>New instance of Serilog with new context</returns>
        public static ILogger AddHiddenPrefix(this ILogger logger, string hiddenPrefix)
        {
            return logger
                .ForContext("hidden_prefix", hiddenPrefix);
        }

        private static bool FormattingIndented { get; set; }

        /// <summary>
        /// Helps to serialize an object to formatted Json
        /// </summary>
        /// <param name="logger">Current instance of Serilog</param>
        /// <param name="enabled">Set to true if you want formatted json. Default value - false</param>
        /// <returns>The same instance of Serilog</returns>
        public static ILogger JsonFormattingIndented(this ILogger logger, bool enabled = false)
        {
            FormattingIndented = enabled;

            return logger;
        }

        /// <summary>
        /// <p>Add a new property with a name KEY to the context and VALUE as a destructured object</p>
        /// <p>Note 1: Payload is visible in the log message only if the log level is Verbose or Debug</p>
        /// <p>Note 2: All properties are visible in the log message only if the log level is Verbose</p>
        /// </summary>
        /// <param name="logger">Current instance of Serilog</param>
        /// <param name="key">Payload name to save as a property</param>
        /// <param name="value">Payload value to save as a property</param>
        /// <returns>New instance of Serilog with new context</returns>
        public static ILogger AddPayload(this ILogger logger, string key, object value)
        {
            if (!key.StartsWith("_"))
                key = "_" + key;

            return FormattingIndented
                ? logger.ForContext(key, JsonConvert.SerializeObject(value, Formatting.Indented), true)
                : logger.ForContext(key, value, true);
        }

        /// <summary>
        /// <p>Helps to fill 'SourceContext' for a logger instance within static class.</p>
        /// <example>
        /// How to add 'SourceContext' to the logger instance with static class
        /// <code>
        /// _logger
        ///     .ForContextStaticClass(typeof(MyStaticClass))
        ///     .Information("--> {myVar}", 123);
        /// OR raw approach
        /// _logger
        ///     .ForContext(Constants.SourceContextPropertyName, typeof(MyStaticClass))
        ///     .Information("--> {myVar}", 123);
        /// </code>
        /// </example>
        /// </summary>
        /// <param name="logger">Current instance of Serilog</param>
        /// <param name="type">the result of typeof(MyStaticClass)</param>
        /// <returns></returns>
        public static ILogger ForContextStaticClass(this ILogger logger, Type type)
        {
            return logger.ForContext(Constants.SourceContextPropertyName, type);
        }
    }
}