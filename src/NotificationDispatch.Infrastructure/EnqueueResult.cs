namespace NotificationDispatch.Infrastructure;

public enum EnqueueResult
{
    /// <summary>New job created and enqueued.</summary>
    Created,
    /// <summary>Duplicate: same key, same request. Existing job returned.</summary>
    Duplicate,
    /// <summary>Conflict: same key, different request body. Reject with 409.</summary>
    Conflict
}
