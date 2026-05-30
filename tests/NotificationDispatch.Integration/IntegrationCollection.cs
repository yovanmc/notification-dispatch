using NotificationDispatch.Integration.Fixtures;

namespace NotificationDispatch.Integration;

[CollectionDefinition("Integration")]
public class IntegrationCollection : ICollectionFixture<RedisFixture>
{
    // Marker class. Causes xUnit to share one RedisFixture instance across all
    // test classes in the "Integration" collection instead of creating one per class.
}
