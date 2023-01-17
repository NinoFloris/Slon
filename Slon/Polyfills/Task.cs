namespace System.Threading.Tasks;

#if NETSTANDARD2_0
public static class TaskExtensions
{
    public static Task<TResult> WaitAsync<TResult>(this Task<TResult> task, CancellationToken cancellationToken)
        => WaitAsync(task, unchecked((uint)-1), cancellationToken);

    public static Task<TResult> WaitAsync<TResult>(this Task<TResult> task, uint millisecondsTimeout, CancellationToken cancellationToken)
    {
        if (task.IsCompleted || (!cancellationToken.CanBeCanceled && millisecondsTimeout == unchecked((uint)-1)))
        {
            return task;
        }

        if (cancellationToken.IsCancellationRequested)
        {
            return Task.FromCanceled<TResult>(cancellationToken);
        }

        if (millisecondsTimeout == 0)
        {
            return Task.FromException<TResult>(new TimeoutException());
        }

        return Core(task, millisecondsTimeout);

        static async Task<TResult> Core(Task<TResult> task, uint millisecondstimeout)
        {
            // We need to be able to cancel the "timeout" task, so create a token source
            var cts = new CancellationTokenSource();

            // Create the timeout task (don't await it)
            var timeoutTask = Task.Delay(TimeSpan.FromMilliseconds(millisecondstimeout), cts.Token);

            // Run the task and timeout in parallel, return the Task that completes first
            var completedTask = await Task.WhenAny(task, timeoutTask).ConfigureAwait(false);

            if (completedTask == task)
            {
                // Cancel the "timeout" task so we don't leak a Timer
                cts.Cancel();
                // await the task to bubble up any errors etc
                return await task.ConfigureAwait(false);
            }

            // Observe the exception.
            var _ = timeoutTask.Exception;
            throw new TimeoutException($"Task timed out after {millisecondstimeout}");
        }
    }
}
#endif
