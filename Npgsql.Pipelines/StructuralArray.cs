// Licensed to the .NET Foundation under one or more agreements.
// The .NET Foundation licenses this file to you under the MIT license.
// See the LICENSE file in the project root for more information.

using System.Collections.Generic;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;

namespace System.Collections.Immutable;

/// <summary>
/// Extensions for <see cref="StructuralArray{T}"/>.
/// </summary>
static class StructuralArray
{
    /// <summary>
    /// Creates an <see cref="StructuralArray{T}"/> instance from a given <see cref="ImmutableArray{T}"/>.
    /// </summary>
    /// <typeparam name="T">The type of items in the input array.</typeparam>
    /// <param name="array">The input <see cref="ImmutableArray{T}"/> instance.</param>
    /// <returns>An <see cref="StructuralArray{T}"/> instance from a given <see cref="ImmutableArray{T}"/>.</returns>
    public static StructuralArray<T> AsStructuralArray<T>(this ImmutableArray<T> array)
        where T : IEquatable<T>
    {
        return new(array);
    }
}

/// <summary>
/// An imutable, equatable array. This is equivalent to <see cref="ImmutableArray{T}"/> but with value equality support.
/// </summary>
/// <typeparam name="T">The type of values in the array.</typeparam>
readonly struct StructuralArray<T> : IEquatable<StructuralArray<T>>, IEnumerable<T>
    where T : IEquatable<T>
{
    /// <summary>
    /// The underlying <typeparamref name="T"/> array.
    /// </summary>
    readonly T[]? array;

    /// <summary>
    /// Creates a new <see cref="StructuralArray{T}"/> instance.
    /// </summary>
    /// <param name="array">The input <see cref="ImmutableArray{T}"/> to wrap.</param>
    public StructuralArray(ImmutableArray<T> array)
    {
        this.array = Unsafe.As<ImmutableArray<T>, T[]?>(ref array);
    }

    /// <summary>
    /// Gets a reference to an item at a specified position within the array.
    /// </summary>
    /// <param name="index">The index of the item to retrieve a reference to.</param>
    /// <returns>A reference to an item at a specified position within the array.</returns>
    public ref readonly T this[int index]
    {
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        get => ref AsImmutableArray().ItemRef(index);
    }

    /// <summary>
    /// Gets a value indicating whether the current array is empty.
    /// </summary>
    public bool IsEmpty
    {
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        get => AsImmutableArray().IsEmpty;
    }

    /// <summary>
    /// Gets a value indicating whether the current array is default or empty.
    /// </summary>
    public bool IsDefaultOrEmpty
    {
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        get => AsImmutableArray().IsDefaultOrEmpty;
    }

    /// <summary>
    /// Gets the length of the current array.
    /// </summary>
    public int Length
    {
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        get => AsImmutableArray().Length;
    }

    /// <sinheritdoc/>
    public bool Equals(StructuralArray<T> array)
    {
        return AsSpan().SequenceEqual(array.AsSpan());
    }

    /// <sinheritdoc/>
    public override bool Equals(object? obj)
    {
        return obj is StructuralArray<T> array && Equals(this, array);
    }

    /// <sinheritdoc/>
    public override unsafe int GetHashCode()
    {
        if (this.array is not T[] array)
        {
            return 0;
        }

        HashCode hashCode = default;

        if (typeof(T) == typeof(byte))
        {
            ReadOnlySpan<T> span = array;
            ref T r0 = ref MemoryMarshal.GetReference(span);
            ref byte r1 = ref Unsafe.As<T, byte>(ref r0);

            fixed (byte* p = &r1)
            {
                ReadOnlySpan<byte> bytes = new(p, span.Length);
#if !NETSTANDARD2_0
                hashCode.AddBytes(bytes);
#else
                foreach (var t in bytes)
                {
                    hashCode.Add(t);
                }
#endif
            }
        }
        else
        {
            foreach (T item in array)
            {
                hashCode.Add(item);
            }
        }

        return hashCode.ToHashCode();
    }

    /// <summary>
    /// Gets an <see cref="ImmutableArray{T}"/> instance from the current <see cref="StructuralArray{T}"/>.
    /// </summary>
    /// <returns>The <see cref="ImmutableArray{T}"/> from the current <see cref="StructuralArray{T}"/>.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public ImmutableArray<T> AsImmutableArray()
    {
        return Unsafe.As<T[]?, ImmutableArray<T>>(ref Unsafe.AsRef(in this.array));
    }

    /// <summary>
    /// Creates an <see cref="StructuralArray{T}"/> instance from a given <see cref="ImmutableArray{T}"/>.
    /// </summary>
    /// <param name="array">The input <see cref="ImmutableArray{T}"/> instance.</param>
    /// <returns>An <see cref="StructuralArray{T}"/> instance from a given <see cref="ImmutableArray{T}"/>.</returns>
    public static StructuralArray<T> FromImmutableArray(ImmutableArray<T> array)
    {
        return new(array);
    }

    /// <summary>
    /// Returns a <see cref="ReadOnlySpan{T}"/> wrapping the current items.
    /// </summary>
    /// <returns>A <see cref="ReadOnlySpan{T}"/> wrapping the current items.</returns>
    public ReadOnlySpan<T> AsSpan()
    {
        return AsImmutableArray().AsSpan();
    }

    /// <summary>
    /// Copies the contents of this <see cref="StructuralArray{T}"/> instance. to a mutable array.
    /// </summary>
    /// <returns>The newly instantiated array.</returns>
    public T[] ToArray()
    {
        return AsImmutableArray().ToArray();
    }

    /// <summary>
    /// Gets an <see cref="ImmutableArray{T}.Enumerator"/> value to traverse items in the current array.
    /// </summary>
    /// <returns>An <see cref="ImmutableArray{T}.Enumerator"/> value to traverse items in the current array.</returns>
    public ImmutableArray<T>.Enumerator GetEnumerator()
    {
        return AsImmutableArray().GetEnumerator();
    }

    /// <sinheritdoc/>
    IEnumerator<T> IEnumerable<T>.GetEnumerator()
    {
        return ((IEnumerable<T>)AsImmutableArray()).GetEnumerator();
    }

    /// <sinheritdoc/>
    IEnumerator IEnumerable.GetEnumerator()
    {
        return ((IEnumerable)AsImmutableArray()).GetEnumerator();
    }

    /// <summary>
    /// Implicitly converts an <see cref="ImmutableArray{T}"/> to <see cref="StructuralArray{T}"/>.
    /// </summary>
    /// <returns>An <see cref="StructuralArray{T}"/> instance from a given <see cref="ImmutableArray{T}"/>.</returns>
    public static implicit operator StructuralArray<T>(ImmutableArray<T> array)
    {
        return FromImmutableArray(array);
    }

    /// <summary>
    /// Implicitly converts an <see cref="StructuralArray{T}"/> to <see cref="ImmutableArray{T}"/>.
    /// </summary>
    /// <returns>An <see cref="ImmutableArray{T}"/> instance from a given <see cref="StructuralArray{T}"/>.</returns>
    public static implicit operator ImmutableArray<T>(StructuralArray<T> array)
    {
        return array.AsImmutableArray();
    }

    /// <summary>
    /// Checks whether two <see cref="StructuralArray{T}"/> values are the same.
    /// </summary>
    /// <param name="left">The first <see cref="StructuralArray{T}"/> value.</param>
    /// <param name="right">The second <see cref="StructuralArray{T}"/> value.</param>
    /// <returns>Whether <paramref name="left"/> and <paramref name="right"/> are equal.</returns>
    public static bool operator ==(StructuralArray<T> left, StructuralArray<T> right)
    {
        return left.Equals(right);
    }

    /// <summary>
    /// Checks whether two <see cref="StructuralArray{T}"/> values are not the same.
    /// </summary>
    /// <param name="left">The first <see cref="StructuralArray{T}"/> value.</param>
    /// <param name="right">The second <see cref="StructuralArray{T}"/> value.</param>
    /// <returns>Whether <paramref name="left"/> and <paramref name="right"/> are not equal.</returns>
    public static bool operator !=(StructuralArray<T> left, StructuralArray<T> right)
    {
        return !left.Equals(right);
    }
}
