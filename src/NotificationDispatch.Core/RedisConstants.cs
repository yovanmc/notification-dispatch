namespace NotificationDispatch.Core;

public static class RedisConstants
{
    public const string JobStreamKey = "notifications:jobs";
    public const string ConsumerGroupName = "worker-group";
    public const string DlqStreamKey = "notifications:dlq";
}
