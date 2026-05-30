using StackExchange.Redis;
using Testcontainers.Redis;

namespace NotificationDispatch.Integration.Fixtures;

public class RedisFixture : IAsyncLifetime
{
    private readonly RedisContainer _container = new RedisBuilder("redis:7-alpine").Build();

    public IConnectionMultiplexer Connection { get; private set; } = null!;
    public string ConnectionString => _container.GetConnectionString();

    public async ValueTask InitializeAsync()
    {
        await _container.StartAsync();
        try
        {
            var options = ConfigurationOptions.Parse(ConnectionString);
            options.AllowAdmin = true;
            Connection = await ConnectionMultiplexer.ConnectAsync(options);
        }
        catch
        {
            await _container.DisposeAsync();
            throw;
        }
    }

    public async Task FlushAsync()
    {
        var server = Connection.GetServer(Connection.GetEndPoints().First());
        await server.FlushDatabaseAsync();
    }

    public async ValueTask DisposeAsync()
    {
        Connection?.Dispose();
        await _container.DisposeAsync();
    }
}
