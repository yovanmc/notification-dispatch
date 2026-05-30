using Microsoft.AspNetCore.Mvc.Testing;
using Testcontainers.Redis;

namespace NotificationDispatch.Integration.Fixtures;

public class AppFixture : IAsyncLifetime
{
    private RedisContainer _redis = null!;
    private WebApplicationFactory<Program> _factory = null!;

    public HttpClient ApiClient { get; private set; } = null!;
    public string RedisConnectionString { get; private set; } = null!;

    public async ValueTask InitializeAsync()
    {
        _redis = new RedisBuilder("redis:7-alpine").Build();
        await _redis.StartAsync();
        RedisConnectionString = _redis.GetConnectionString();

        _factory = new WebApplicationFactory<Program>()
            .WithWebHostBuilder(b =>
            {
                b.UseSetting("Redis:ConnectionString", RedisConnectionString);
            });

        ApiClient = _factory.CreateClient();
    }

    public async ValueTask DisposeAsync()
    {
        ApiClient.Dispose();
        await _factory.DisposeAsync();
        await _redis.DisposeAsync();
    }
}
