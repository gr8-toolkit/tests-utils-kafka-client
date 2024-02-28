using System.Linq;
using Serilog.Core;
using Serilog.Events;

namespace GR8Tech.TestUtils.KafkaClient.Common.Logging
{
    internal static class CustomSerilogFunctions
    {
        public static LogEventPropertyValue Payload(LogEvent logEvent)
        {
            return new StructureValue(logEvent.Properties
                .Where(kvp => kvp.Key.StartsWith("_"))
                .Select(kvp => new LogEventProperty(kvp.Key, kvp.Value)));
        }

        public static LogEventPropertyValue PayloadSize(LogEvent logEvent)
        {
            return new ScalarValue(logEvent.Properties
                .Where(kvp => kvp.Key.StartsWith("_"))
                .Select(kvp => new LogEventProperty(kvp.Key, kvp.Value))
                .Count());
        }

        public static LogEventPropertyValue PropertiesSize(LogEvent logEvent)
        {
            return new ScalarValue(logEvent.Properties.Count());
        }

        public static LogEventPropertyValue IsDebugActive(LogEvent logEvent)
        {
            var debug = false;

            if (logEvent.Properties.ContainsKey(Constants.SourceContextPropertyName))
            {
                var sv = (ScalarValue)logEvent.Properties.First(x => x.Key == Constants.SourceContextPropertyName)
                    .Value;
                var newLogger =
                    SerilogDecorator.Logger.ForContext(Constants.SourceContextPropertyName, (string)sv.Value);
                debug = newLogger.IsEnabled(LogEventLevel.Debug);
            }
            else
                debug = SerilogDecorator.Logger.IsEnabled(LogEventLevel.Debug);

            if (debug)
                return new ScalarValue(true);

            return new ScalarValue(false);
        }

        public static LogEventPropertyValue IsVerboseActive(LogEvent logEvent)
        {
            var verbose = false;

            if (logEvent.Properties.ContainsKey(Constants.SourceContextPropertyName))
            {
                var sv = (ScalarValue)logEvent.Properties.First(
                    x => x.Key == Constants.SourceContextPropertyName).Value;
                var newLogger =
                    SerilogDecorator.Logger.ForContext(Constants.SourceContextPropertyName, (string)sv.Value);
                verbose = newLogger.IsEnabled(LogEventLevel.Verbose);
            }
            else
                verbose = SerilogDecorator.Logger.IsEnabled(LogEventLevel.Verbose);

            if (verbose)
                return new ScalarValue(true);

            return new ScalarValue(false);
        }
    }
}