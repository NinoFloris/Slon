using System;
using System.Collections;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace Npgsql.Pipelines.Protocol.PgV3;

readonly struct Command
{
    readonly ICommandInfo _commandInfo;
    readonly IOCompletionPair _completionPair;

    Command(ICommandInfo commandInfo, IOCompletionPair completionPair)
    {
        _commandInfo = commandInfo;
        _completionPair = completionPair;
    }

    public ValueTask<Operation> GetProtocol() => _completionPair.SelectAsync();
    public ICommandInfo CommandInfo => _commandInfo;
    public ValueTask<WriteResult> WriteTask => _completionPair.Write;
    public static Command Create(ICommandInfo commandInfo, IOCompletionPair completionPair)
        => new(commandInfo, completionPair);
}

readonly struct CommandBatch: IEnumerable<Command>
{
    readonly Command[] _commands;

    CommandBatch(Command[] commands)
    {
        _commands = commands;
    }

    public static CommandBatch Create(params Command[] commands)
        => new(commands);

    public static CommandBatch Create(Command command)
        => new(new[] { command });

    public int Length => _commands.Length;

    public struct Enumerator: IEnumerator<Command>
    {
        readonly CommandBatch _batch;
        Command _current;
        int _index;

        internal Enumerator(CommandBatch batch)
        {
            _batch = batch;
            _index = 0;
        }

        public bool MoveNext()
        {
            var commands = _batch._commands;
            if (commands is null)
                throw new InvalidOperationException("Invalid enumerator.");

            if ((uint)_index < (uint)commands.Length)
            {
                _current = _batch._commands[_index];
                _index++;
                return true;
            }

            _current = default;
            _index = commands.Length + 1;
            return false;
        }

        public readonly Command Current => _current;

        public void Reset()
        {
            _index = 0;
            _current = default;
        }

        readonly object IEnumerator.Current => Current;
        public void Dispose() { }
    }

    public Enumerator GetEnumerator() => new(this);
    IEnumerator<Command> IEnumerable<Command>.GetEnumerator() => GetEnumerator();
    IEnumerator IEnumerable.GetEnumerator() => GetEnumerator();
}
