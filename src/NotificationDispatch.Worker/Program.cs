using NotificationDispatch.Core.Interfaces;
using NotificationDispatch.Infrastructure;
using NotificationDispatch.Worker.Senders;
using NotificationDispatch.Worker.Services;
using Serilog;
using StackExchange.Redis;

Log.Logger = new LoggerConfiguration()
    .WriteTo.Console(outputTemplate:
        "[{Timestamp:HH:mm:ss} {Level:u3}] {Message:lj}{NewLine}{Exception}")
    .Enrich.FromLogContext()
    .CreateLogger();

try
{
    var builder = Host.CreateApplicationBuilder(args);
    builder.Services.AddSerilog(Log.Logger);

    var redisConnection = builder.Configuration.GetValue<string>("Redis:ConnectionString")
        ?? "localhost:6379";

    builder.Services.AddSingleton<IConnectionMultiplexer>(
        ConnectionMultiplexer.Connect(redisConnection));

    builder.Services.AddSingleton<RedisStatusStore>();
    builder.Services.AddSingleton<DeadLetterStore>();
    builder.Services.AddSingleton<RetryPolicy>();

    builder.Services.AddSingleton<INotificationSender, EmailSender>();
    builder.Services.AddSingleton<INotificationSender, SmsSender>();
    builder.Services.AddHttpClient<WebhookSender>()
        .ConfigureHttpClient(c => c.Timeout = TimeSpan.FromSeconds(30));
    builder.Services.AddSingleton<INotificationSender>(sp => sp.GetRequiredService<WebhookSender>());

    builder.Services.AddSingleton<SenderRouter>(sp =>
        new SenderRouter(sp.GetServices<INotificationSender>()));

    builder.Services.AddHostedService<NotificationWorkerService>();

    var host = builder.Build();
    await host.RunAsync();
}
catch (Exception ex)
{
    Log.Fatal(ex, "Worker terminated unexpectedly");
}
finally
{
    await Log.CloseAndFlushAsync();
}
