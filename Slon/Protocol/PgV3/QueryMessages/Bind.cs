using System;
using System.Buffers;
using System.Diagnostics;
using System.Runtime.CompilerServices;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Slon.Buffers;
using Slon.Protocol.Pg;
using Slon.Protocol.PgV3.Descriptors;
using Parameter = Slon.Protocol.Pg.Parameter;

namespace Slon.Protocol.PgV3;

static class FormatCodeExtensions
{
    public static FormatCode GetFormatCode(this Pg.Parameter p) => p.HasTextWrite() ? FormatCode.Text : FormatCode.Binary;
}

readonly struct Bind: IFrontendMessage
{
    readonly string _portalName;
    readonly ParametersWriter _parametersWriter;
    readonly FormatCode? _parametersForAllCode;
    readonly RowRepresentation _rowRepresentation;
    readonly Encoding _encoding;
    readonly SizedString _preparedStatementName;
    readonly int _precomputedMessageLength;

    public Bind(string portalName, ParametersWriter parametersWriter, RowRepresentation rowRepresentation, SizedString? preparedStatementName, Encoding encoding)
    {
        if (parametersWriter.Items.Length > StatementParameter.MaxCount)
            // Throw inlined as constructors will never be inlined.
            throw new InvalidOperationException($"Cannot accept more than ushort.MaxValue ({StatementParameter.MaxCount} parameters.");

        if (rowRepresentation is { IsPerColumn: true, PerColumn.Length: > StatementField.MaxCount })
            // Throw inlined as constructors will never be inlined.
            throw new InvalidOperationException($"Cannot accept more than short.MaxValue ({StatementField.MaxCount} result columns.");

        if (!parametersWriter.Items.IsEmpty)
            if (parametersWriter.Context.HasTextWrites())
            {
                var forall = true;
                FormatCode? formatCode = parametersWriter.Items.IsEmpty ? null : parametersWriter.Items.Span[0].GetFormatCode();
                // Note i = 1 to start at the second param.
                for (var i = 1; i < parametersWriter.Items.Length; i++)
                {
                    if (formatCode != parametersWriter.Items.Span[i].GetFormatCode())
                    {
                        forall = false;
                        break;
                    }
                }

                if (forall)
                    _parametersForAllCode = formatCode;
            }
            else
                _parametersForAllCode = FormatCode.Binary;

        _portalName = portalName;
        _parametersWriter = parametersWriter;
        _rowRepresentation = rowRepresentation;
        _encoding = encoding;
        _preparedStatementName = preparedStatementName ?? string.Empty;
        _precomputedMessageLength = ComputeMessageLength();
        Debug.Assert(_precomputedMessageLength == -1 && !_parametersWriter.Items.IsEmpty || _precomputedMessageLength > 0);
    }

    public bool CanWrite => _precomputedMessageLength is -1 or <= 8192;

    public void Write<T>(ref BufferWriter<T> buffer) where T : IBufferWriter<byte>
    {
        var bufferStart = buffer;
        if (_precomputedMessageLength != -1)
            PgV3FrontendHeader.WriteHeader(ref buffer, FrontendCode.Bind, _precomputedMessageLength);
        else
            buffer.Advance(PgV3FrontendHeader.ByteCount);

        buffer.WriteCString(_portalName, _encoding);
        buffer.WriteCString(_preparedStatementName, _encoding);

        var enumerator = _parametersWriter;
        var parameters = enumerator.Items;
        // Parameter format codes.
        if (parameters.Length == 0)
        {
            buffer.WriteShort(0);
        }
        else if (_parametersForAllCode is not null)
        {
            buffer.WriteShort(1);
            buffer.WriteShort((short)_parametersForAllCode);
        }
        else
        {
            buffer.WriteUShort((ushort)parameters.Length);
            foreach (var parameter in parameters.Span)
                buffer.WriteShort((short)parameter.GetFormatCode());
        }

        // Parameter values.
        buffer.WriteUShort((ushort)parameters.Length);
        if (!parameters.IsEmpty)
        {
            if (_precomputedMessageLength == -1)
            {
                var parametersByteCount = _parametersWriter.WriteOnePass(ref buffer);
                PgV3FrontendHeader.WriteHeader(ref bufferStart, FrontendCode.Bind, ComputeMessageLength(parametersByteCount));
            }
            else
            {
                _parametersWriter.Write(ref buffer);
            }
        }

        // Result column format codes.
        if (_rowRepresentation.IsForAll)
        {
            buffer.WriteShort(1);
            buffer.WriteShort((short)_rowRepresentation.ForAll.ToFormatCode());
        }
        else
        {
            buffer.WriteShort((short)_rowRepresentation.PerColumn.Length);
            foreach (var repr in _rowRepresentation.PerColumn.Span)
                buffer.WriteShort((short)repr.ToFormatCode());
        }
    }

    public async ValueTask<FlushResult> WriteAsync<T>(MessageWriter<T> writer, CancellationToken cancellationToken = default) where T : IStreamingWriter<byte>
    {
        Debug.Assert(_precomputedMessageLength != -1);
        PgV3FrontendHeader.WriteHeader(ref writer.Writer, FrontendCode.Bind, _precomputedMessageLength);

        writer.WriteByte((byte)FrontendCode.Bind);
        writer.WriteInt(_precomputedMessageLength + MessageWriter.IntByteCount);

        writer.WriteCString(_portalName, _encoding);
        writer.WriteCString(_preparedStatementName, _encoding);

        WriteParameterCodes(ref writer.Writer);

        var enumerator = _parametersWriter;
        var parameters = enumerator.Items;
        writer.WriteUShort((ushort)parameters.Length);
        if (!parameters.IsEmpty)
            await _parametersWriter.WriteStreaming(writer, cancellationToken).ConfigureAwait(false);

        WriteResultColumnCodes(ref writer.Writer);
        return new FlushResult(isCanceled: false, isCompleted: false);
    }

    int ComputeMessageLength(int? parametersByteCount = null)
    {
        var parameters = _parametersWriter.Items;
        var psByteCount = parametersByteCount.GetValueOrDefault();
        if (parametersByteCount is null && !_parametersWriter.TryGetTotalByteCount(out psByteCount))
            return -1;

        return
            MessageWriter.GetCStringByteCount(_portalName, _encoding) +
            MessageWriter.GetCStringByteCount(_preparedStatementName, _encoding) +
            MessageWriter.ShortByteCount + // Number of parameter codes
            (_parametersForAllCode is not null ? MessageWriter.ShortByteCount : parameters.Length * MessageWriter.ShortByteCount) +
            MessageWriter.ShortByteCount + // Number of parameter values
            psByteCount +
            (_rowRepresentation.IsForAll
                ? MessageWriter.ShortByteCount * 2
                : MessageWriter.ShortByteCount + _rowRepresentation.PerColumn.Length * MessageWriter.ShortByteCount);
    }

    // Separate method, async methods can't enumerate spans.
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    void WriteParameterCodes<T>(ref StreamingWriter<T> writer) where T : IStreamingWriter<byte>
    {
        var parameters = _parametersWriter.Items;
        if (parameters.Length == 0)
        {
            writer.WriteShort(0);
            return;
        }

        if (_parametersForAllCode is not null)
        {
            writer.WriteShort(1);
            writer.WriteShort((short)_parametersForAllCode);
        }
        else
        {
            writer.WriteUShort((ushort)parameters.Length);
            foreach (var parameter in parameters.Span)
                writer.WriteShort((short)parameter.GetFormatCode());
        }
    }

    // Separate method, async methods can't enumerate spans.
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    void WriteResultColumnCodes<T>(ref StreamingWriter<T> writer) where T : IStreamingWriter<byte>
    {
        if (_rowRepresentation.IsForAll)
        {
            writer.WriteShort(1);
            writer.WriteShort((short)_rowRepresentation.ForAll.ToFormatCode());
        }
        else
        {
            writer.WriteShort((short)_rowRepresentation.PerColumn.Length);
            foreach (var repr in _rowRepresentation.PerColumn.Span)
                writer.WriteShort((short)repr.ToFormatCode());
        }
    }
}
