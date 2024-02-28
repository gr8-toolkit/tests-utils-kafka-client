using System.IO;
using Microsoft.Extensions.Configuration;
using Serilog;
using Serilog.Exceptions;
using Serilog.Expressions;
using Serilog.Templates;
using Serilog.Templates.Themes;
using static System.Environment;

namespace GR8Tech.TestUtils.KafkaClient.Common.Logging
{
    public static class SerilogDecorator
    {
        /// <summary>
        /// <p>Logger is based on Serilog, which writes logs to the Console in a defined format with the help of ExpressionTemplate.</p>
        /// <p>Additional information on each log Level is handled based on the logger Level from the current context.</p>
        /// <example>
        /// Create a log with a payload
        /// <code>
        /// _logger
        ///     .ForContext("_myPayload", payload, true)
        ///     .Information("--> {myVar}", 123);
        /// </code>
        /// <p> </p>
        /// Create the log with a prefix
        /// <code>
        /// _logger
        ///     .ForContext("prefix", "my prefix")
        ///     .Information("--> {myVar}", 123);
        /// </code>
        /// <p> </p>
        /// Create the log with a hidden prefix
        /// <code>
        /// _logger
        ///     .ForContext("hidden_prefix", "my hidden prefix")
        ///     .Information("--> {myVar}", 123);
        /// </code>
        /// </example>
        /// </summary>
        public static ILogger Logger { get; set; }

        static SerilogDecorator()
        {
            var configFileName = File.Exists("test-settings.json") ? "test-settings" : "appsettings";
            var config = new ConfigurationBuilder()
                .SetBasePath(Directory.GetCurrentDirectory())
                .AddJsonFile($"{configFileName}.json", true, true)
                .AddJsonFile($"{configFileName}.{GetEnvironmentVariable("ASPNETCORE_ENVIRONMENT")}.json", true)
                .AddEnvironmentVariables()
                .Build();

            var customSerilogFunctions = new StaticMemberNameResolver(typeof(CustomSerilogFunctions));

            Logger = new LoggerConfiguration()
                .ReadFrom.Configuration(config)
                .WriteTo.Console(new ExpressionTemplate(
                    "[{UtcDateTime(@t):o}] " +
                    "[{@l:u3}] " +
                    "{#if SourceContext is not null and IsDebugActive()}[{SourceContext}] {#end}" +
                    "{#if prefix is not null}[{prefix}] {#end}" +
                    "{#if hidden_prefix is not null and IsDebugActive()}[{hidden_prefix}] {#end}" +
                    "{@m}\n" +
                    "{#if PayloadSize() > 0 and IsDebugActive() and not IsVerboseActive()}" +
                        " Payload:\n{#each name, value in Payload()}   {name} = {value}\n{#end}" +
                    "{#end}" +
                    "{#if PropertiesSize() > 0 and IsVerboseActive()}" +
                        " Properties:\n{#each name, value in Rest()}   {name} = {value}\n{#end}" +
                    "{#end}" +
                    "{#if @x is not null} {@x}{#end}",
                    theme: TemplateTheme.Code,
                    nameResolver: customSerilogFunctions))
                .Enrich.FromLogContext()
                .Enrich.WithExceptionDetails()
                .Enrich.WithThreadId()
                .Enrich.WithMachineName()
                .CreateLogger(); 
        }
    }
}