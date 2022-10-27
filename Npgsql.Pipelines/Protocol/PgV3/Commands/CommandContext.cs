using System;
using System.Collections;
using System.Collections.Generic;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Threading.Tasks;

namespace Npgsql.Pipelines.Protocol.PgV3.Commands;

readonly struct CommandContext
{
    readonly IOCompletionPair _completionPair;
    readonly ExecutionFlags _flags;
    readonly ICommandSession? _session;

    CommandContext(IOCompletionPair completionPair, ExecutionFlags flags, ICommandSession? session)
    {
        _completionPair = completionPair;
        _flags = flags;
        _session = session;
    }

    public ExecutionFlags ExecutionFlags => _session?.ExecutionFlags ?? _flags;
    public bool IsCompleted => _completionPair.Read is { IsCompletedSuccessfully: true, Result.IsCompleted: true };
    public ValueTask<WriteResult> WriteTask => _completionPair.Write;
    public ValueTask<Operation> GetOperation() => _completionPair.SelectAsync();

    public ICommandSession? Session => _session;

    public static CommandContext Create(IOCompletionPair completionPair, ExecutionFlags flags, ICommandSession? session)
        => new(completionPair, flags, session);
}

readonly struct CommandContextBatch: IEnumerable<CommandContext>
{
    readonly CommandContext _context;
    readonly CommandContext[]? _contexts;

    CommandContextBatch(CommandContext[] contexts)
        => _contexts = contexts;

    CommandContextBatch(CommandContext context)
        => _context = context;

    public static CommandContextBatch Create(params CommandContext[] contexts)
        => new(contexts);

    public static CommandContextBatch Create(CommandContext context)
    {
#if !NETSTANDARD2_0
        return new(context);
#else
        // MemoryMarshal.CreateReadOnlySpan cannot be implemented safely (could make two codepaths for the enumerator, but it's ns2.0 so who cares).
        return new(new[] { context });
#endif
    }

    public int Length => _contexts?.Length ?? 1;

    ReadOnlySpan<CommandContext> Contexts
    {
        get
        {
#if !NETSTANDARD2_0
            return _contexts ?? MemoryMarshal.CreateReadOnlySpan(ref Unsafe.AsRef(_context), 1);
#else
            return _contexts!;
#endif
        }
    }

    public bool AllCompleted
    {
        get
        {
            foreach (var command in Contexts)
            {
                if (!command.GetOperation().IsCompleted)
                    return false;
            }

            return true;
        }
    }

    public struct Enumerator: IEnumerator<CommandContext>
    {
        readonly CommandContext[]? _contexts;
        CommandContext _current;
        int _index;

        internal Enumerator(CommandContextBatch instance)
        {
            if (_contexts is null)
            {
                _current = instance._context;
                _index = -2;
            }
            else
            {
                _contexts = instance._contexts;
                _index = 0;
            }
        }

        public bool MoveNext()
        {
            var contexts = _contexts;
            // Single element case.
            if (contexts is null)
            {
                if (_index == -1)
                    return false;

                if (_index != -2)
                    throw new InvalidOperationException("Invalid Enumerator, default value?");

                _index++;
                return true;
            }

            if ((uint)_index < (uint)contexts.Length)
            {
                _current = contexts[_index];
                _index++;
                return true;
            }

            _current = default;
            _index = contexts.Length + 1;
            return false;
        }

        public readonly CommandContext Current => _current;

        public void Reset()
        {
            if (_contexts is null)
            {
                _index = -2;
            }
            else
            {
                _index = 0;
                _current = default;
            }
        }

        readonly object IEnumerator.Current => Current;
        public void Dispose() { }
    }

    public Enumerator GetEnumerator() => new(this);
    IEnumerator<CommandContext> IEnumerable<CommandContext>.GetEnumerator() => GetEnumerator();
    IEnumerator IEnumerable.GetEnumerator() => GetEnumerator();
}
