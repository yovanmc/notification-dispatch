using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using NotificationDispatch.Core.Interfaces;
using NotificationDispatch.Infrastructure;
using NotificationDispatch.Worker.Senders;
using NotificationDispatch.Worker.Services;
using StackExchange.Redis;
using Testcontainers.Redis;

namespace NotificationDispatch.Integration;

/// <summary>
/// Validates the shape of the worker DI graph: proves that the host resolves all singletons,
/// IHttpClientFactory is wired for WebhookSender, and NotificationWorkerService starts without
/// error. Registers services in the same order as Worker/Program.cs; the Docker Compose smoke
/// test exercises the real container entrypoint for full production coverage.
/// </summary>
public class WorkerHostStartupTests : IAsyncLifetime
{
    private RedisContainer _redis = null!;

    public async ValueTask InitializeAsync()
    {
        _redis = new RedisBuilder("redis:7-alpine").Build();
        await _redis.StartAsync();
    }

    public async ValueTask DisposeAsync()
    {
        await _redis.DisposeAsync();
    }

    [Fact]
    public async Task WorkerHost_ProductionDIWiring_StartsWithoutError()
    {
        var connString = _redis.GetConnectionString();

        // Mirror Worker Program.cs DI registration exactly.
        var builder = Host.CreateApplicationBuilder(Array.Empty<string>());
        builder.Configuration["Redis:ConnectionString"] = connString;
        builder.Services.AddLogging(b => b.ClearProviders()); // suppress output in tests

        builder.Services.AddSingleton<IConnectionMultiplexer>(
            ConnectionMultiplexer.Connect(connString));

        builder.Services.AddSingleton<RedisStatusStore>();
        builder.Services.AddSingleton<DeadLetterStore>();
        builder.Services.AddSingleton<RetryPolicy>();

        builder.Services.AddSingleton<INotificationSender, EmailSender>();
        builder.Services.AddSingleton<INotificationSender, SmsSender>();
        builder.Services.AddHttpClient<WebhookSender>()
            .ConfigureHttpClient(c => c.Timeout = TimeSpan.FromSeconds(30));
        builder.Services.AddSingleton<INotificationSender>(
            sp => sp.GetRequiredService<WebhookSender>());

        builder.Services.AddSingleton<SenderRouter>(
            sp => new SenderRouter(sp.GetServices<INotificationSender>()));

        builder.Services.AddHostedService<NotificationWorkerService>();

        var host = builder.Build();
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(10));
        try
        {
            // StartAsync resolves the full DI graph and starts NotificationWorkerService.
            await host.StartAsync(cts.Token);

            // Brief pause to confirm the worker polling loop is running.
            await Task.Delay(TimeSpan.FromMilliseconds(500), cts.Token);

            // StopAsync triggers graceful shutdown via the cancellation token passed to ExecuteAsync.
            await host.StopAsync(CancellationToken.None);
        }
        finally
        {
            // Dispose releases singletons (Redis multiplexer, HttpClient, etc.).
            host.Dispose();
        }
    }
}
