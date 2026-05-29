using NotificationDispatch.Core.Interfaces;
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
    var builder = Host.CreateDefaultBuilder(args)
        .UseSerilog()
        .ConfigureServices((context, services) =>
        {
            var redisConnection = context.Configuration.GetValue<string>("Redis:ConnectionString")
                ?? "localhost:6379";

            services.AddSingleton<IConnectionMultiplexer>(
                ConnectionMultiplexer.Connect(redisConnection));

            services.AddSingleton<RedisStatusStore>();
            services.AddSingleton<DeadLetterStore>();
            services.AddSingleton<RetryPolicy>();

            services.AddSingleton<INotificationSender, EmailSender>();
            services.AddSingleton<INotificationSender, SmsSender>();
            services.AddHttpClient<WebhookSender>()
                .ConfigureHttpClient(c => c.Timeout = TimeSpan.FromSeconds(30));
            services.AddSingleton<INotificationSender>(sp => sp.GetRequiredService<WebhookSender>());

            services.AddSingleton<SenderRouter>(sp =>
                new SenderRouter(sp.GetServices<INotificationSender>()));

            services.AddHostedService<NotificationWorkerService>();
        });

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
