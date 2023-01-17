using System;
using System.Collections;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace Npgsql.Pipelines.Protocol;

// Used to link up CommandContexts constructed before a session is available with an actual session or statement later on.
interface ICommandExecutionProvider<TExecution>
{
    public TExecution Get(in CommandContext<TExecution> context);
}

readonly struct CommandContext<TExecution>
{
    readonly IOCompletionPair _completionPair;
    readonly TExecution _commandExecution;
    readonly ICommandExecutionProvider<TExecution>? _provider;

    CommandContext(IOCompletionPair completionPair, ICommandExecutionProvider<TExecution> provider)
    {
        _completionPair = completionPair;
        _provider = provider;
        _commandExecution = default!;
    }

    CommandContext(IOCompletionPair completionPair, TExecution commandExecution)
    {
        _completionPair = completionPair;
        _commandExecution = commandExecution;
    }

    // Copy constructor
    CommandContext(IOCompletionPair completionPair, TExecution commandExecution, ICommandExecutionProvider<TExecution>? provider)
    {
        _completionPair = completionPair;
        _commandExecution = commandExecution;
        _provider = provider;
    }

    /// Only reliable to be called once, store the result if multiple lookups are needed.
    public TExecution GetCommandExecution()
    {
        if (_provider is { } provider)
            return provider.Get(this);

        return _commandExecution;
    }

    public bool IsCompleted => _completionPair.ReadSlot.IsCompleted;
    public ValueTask<WriteResult> WriteTask => _completionPair.Write;
    public OperationSlot ReadSlot => _completionPair.ReadSlot;

    public ValueTask<Operation> GetOperation() => _completionPair.SelectAsync();

    public CommandContext<TExecution> WithIOCompletionPair(IOCompletionPair completionPair)
        => new(completionPair, _commandExecution, _provider);

    public static CommandContext<TExecution> Create(IOCompletionPair completionPair, ICommandExecutionProvider<TExecution> provider)
        => new(completionPair, provider);

    public static CommandContext<TExecution> Create(IOCompletionPair completionPair, TExecution commandExecution)
        => new(completionPair, commandExecution);
}

readonly struct CommandContextBatch<TExecution>: IEnumerable<CommandContext<TExecution>>
{
    readonly CommandContext<TExecution> _context;
    readonly CommandContext<TExecution>[]? _contexts;

    CommandContextBatch(CommandContext<TExecution>[] contexts)
    {
        if (contexts.Length == 0)
            // Throw inlined as constructors will never be inlined.
            throw new ArgumentException("Array cannot be empty.", nameof(contexts));

        _contexts = contexts;
    }

    CommandContextBatch(CommandContext<TExecution> context)
        => _context = context;

    public static CommandContextBatch<TExecution> Create(params CommandContext<TExecution>[] contexts)
        => new(contexts);

    public static CommandContextBatch<TExecution> Create(CommandContext<TExecution> context)
    {
#if !NETSTANDARD2_0
        return new(context);
#else
        // MemoryMarshal.CreateReadOnlySpan cannot be implemented safely (could make two codepaths for the enumerator, but it's ns2.0 so who cares).
        return new(new[] { context });
#endif
    }

    public static implicit operator CommandContextBatch<TExecution>(CommandContext<TExecution> commandContext) => Create(commandContext);

    public int Length => _contexts?.Length ?? 1;

    public bool AllCompleted
    {
        get
        {
#if !NETSTANDARD2_0
            var contexts = _contexts ?? new ReadOnlySpan<CommandContext<TExecution>>(_context);
#else
            var contexts = _contexts!;
#endif

            foreach (var command in contexts)
            {
                var op = command.GetOperation();
                if (!op.IsCompleted || !op.Result.IsCompleted)
                    return false;
            }

            return true;
        }
    }

    public struct Enumerator: IEnumerator<CommandContext<TExecution>>
    {
        readonly CommandContext<TExecution>[]? _contexts;
        CommandContext<TExecution> _current;
        int _index;

        internal Enumerator(CommandContextBatch<TExecution> instance)
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
                    ThrowInvalidEnumerator();

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

            static void ThrowInvalidEnumerator() => throw new InvalidOperationException("Invalid Enumerator, default value?");
        }

        public readonly CommandContext<TExecution> Current => _current;

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
    IEnumerator<CommandContext<TExecution>> IEnumerable<CommandContext<TExecution>>.GetEnumerator() => GetEnumerator();
    IEnumerator IEnumerable.GetEnumerator() => GetEnumerator();
}
