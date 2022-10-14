namespace System.Threading;

#if NETSTANDARD2_0
static class CancellationTokenExtensions
{
    public static CancellationTokenRegistration UnsafeRegister(this CancellationToken cancellationToken, Action<object?> callback, object? state)
    {
        return cancellationToken.Register(callback, state);
    }

    public static CancellationTokenRegistration UnsafeRegister(this CancellationToken cancellationToken, Action<object?, CancellationToken> callback, object? state)
    {
        return cancellationToken.Register(state => callback.Invoke(state, cancellationToken), state);
    }
}

static class CancellationTokenSourceExtensions
{
    public static bool TryReset(this CancellationTokenSource cancellationTokenSource)
    {
        // Best effort, as we can't actually reset the registrations (sockets do correctly unregister when they complete).
        // Npgsql would have the same issues otherwise.
        cancellationTokenSource.CancelAfter(Timeout.Infinite);
        return !cancellationTokenSource.IsCancellationRequested;
    }
}
#endif
