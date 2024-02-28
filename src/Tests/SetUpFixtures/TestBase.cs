using NUnit.Framework;

namespace Tests.SetUpFixtures;

[TestFixture]
[Category("Integration")]
[Parallelizable(ParallelScope.All)]
public class TestBase
{
    internal static string GetNewGuid()
    {
        return Guid.NewGuid().ToString("N");
    }

    internal static int RandomInt(int min = 0, int max = 100)
    {
        return new Random().Next(min, max);
    }
}