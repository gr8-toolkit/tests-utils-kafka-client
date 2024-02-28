using GR8Tech.TestUtils.KafkaClient.Common.Logging;
using NUnit.Framework;
using Serilog;
using Tests.Infrastructure;
using Tests.SetUpFixtures;

namespace Tests;

[SetUpFixture]
public static class GlobalSetUp
{
    private static ILogger _logger = SerilogDecorator.Logger.ForContextStaticClass(typeof(GlobalSetUp));
    
    private static DockerFixture DockerFixture { get; set; }

    [OneTimeSetUp]
    public static async Task SetUp()
    {
        Log.Logger = SerilogDecorator.Logger;
        
        DockerFixture = new DockerFixture();
        KafkaConsumers.StartAll();
        
        _logger.Information("Kafka has started in Docker container");
    }

    [OneTimeTearDown]
    public static void TearDown()
    {
        _logger.Information("Global OneTimeTearDown");
        
        KafkaProducers.DisposeAll();
        KafkaConsumers.DisposeAll();
        DockerFixture.Dispose();
    }
}