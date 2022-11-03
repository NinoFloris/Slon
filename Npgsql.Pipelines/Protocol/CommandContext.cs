using System;
using System.Collections;
using System.Collections.Generic;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Threading.Tasks;
using Npgsql.Pipelines.Protocol.PgV3.Commands;

namespace Npgsql.Pipelines.Protocol;

readonly struct CommandContext
{
    readonly IOCompletionPair _completionPair;
    readonly ExecutionFlags _executionFlags;
    readonly object? _providerOrSessionOrStatement;

    CommandContext(IOCompletionPair completionPair, ICommandExecutionProvider provider)
    {
        _completionPair = completionPair;
        _providerOrSessionOrStatement = provider;
    }

    CommandContext(IOCompletionPair completionPair, CommandExecution commandExecution)
    {
        _completionPair = completionPair;
        // We unpack to save space.
        _executionFlags = commandExecution.TryGetSessionOrStatement(out var session, out var statement);
        _providerOrSessionOrStatement = (object?)session ?? statement;
    }

    // Copy constructor
    CommandContext(IOCompletionPair completionPair, ExecutionFlags executionFlags, object? providerOrSessionOrStatement)
    {
        _completionPair = completionPair;
        _executionFlags = executionFlags;
        _providerOrSessionOrStatement = providerOrSessionOrStatement;
    }

    /// Only reliable to be called once, cache the result if multiple lookups are needed.
    public CommandExecution GetCommandExecution()
        => _providerOrSessionOrStatement switch
        {
            ICommandExecutionProvider provider => provider.Get(this),
            ICommandSession session => CommandExecution.Create(_executionFlags, session),
            Statement statement => CommandExecution.Create(_executionFlags, statement),
            _ => CommandExecution.Create(_executionFlags)
        };

    public bool IsCompleted => _completionPair.ReadSlot.IsCompleted;
    public ValueTask<WriteResult> WriteTask => _completionPair.Write;
    public OperationSlot ReadSlot => _completionPair.ReadSlot;

    /// Only safe to be awaited once, multiple calls to retrieve any pending write status are allowed.
    public ValueTask<Operation> GetOperation() => _completionPair.SelectAsync();

    public CommandContext WithIOCompletionPair(IOCompletionPair completionPair)
        => new(completionPair, _executionFlags, _providerOrSessionOrStatement);

    public static CommandContext Create(IOCompletionPair completionPair, ICommandExecutionProvider provider)
        => new(completionPair, provider);

    public static CommandContext Create(IOCompletionPair completionPair, CommandExecution commandExecution)
        => new(completionPair, commandExecution);
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

    public static implicit operator CommandContextBatch(CommandContext commandContext) => Create(commandContext);

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
