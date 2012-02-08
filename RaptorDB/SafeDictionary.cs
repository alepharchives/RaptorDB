﻿using System;
using System.Collections.Generic;
using System.Runtime.InteropServices;
using System.Text;

namespace RaptorDB
{
    internal class SafeDictionary<TKey, TValue>
    {
        private readonly object _Padlock = new object();
        private readonly Dictionary<TKey, TValue> _Dictionary =null;//= new Dictionary<TKey, TValue>();

        public SafeDictionary(int capacity)
        {
            _Dictionary = new Dictionary<TKey, TValue>(capacity);
        }

        public SafeDictionary()
        {
            _Dictionary = new Dictionary<TKey, TValue>();
        }

        public bool TryGetValue(TKey key, out TValue value)
        {
            return _Dictionary.TryGetValue(key, out value);
        }

        public TValue this[TKey key]
        {
            get
            {
                return _Dictionary[key];
            }
            set
            {
                lock (_Padlock)
                    _Dictionary[key] = value;
            }
        }

        public int Count
        {
            get { return _Dictionary.Count; }
        }

        public ICollection<KeyValuePair<TKey, TValue>> GetList()
        {
            return (ICollection<KeyValuePair<TKey, TValue>>)_Dictionary;
        }

        public IEnumerator<KeyValuePair<TKey, TValue>> GetEnumerator()
        {
            return ((ICollection<KeyValuePair<TKey, TValue>>)_Dictionary).GetEnumerator();
        }

        public void Add(TKey key, TValue value)
        {
            lock (_Padlock)
                _Dictionary.Add(key, value);
        }

        public TKey[] Keys()
        {
            lock (_Padlock)
            {
                TKey[] keys = new TKey[_Dictionary.Keys.Count];
                _Dictionary.Keys.CopyTo(keys, 0);
                return keys;
            }
        }

        public bool Remove(TKey key)
        {
            lock (_Padlock)
                return _Dictionary.Remove(key);
        }
    }

    //------------------------------------------------------------------------------------------------------------------

    internal static class FastDateTime
    {
        public static TimeSpan LocalUtcOffset;

        public static DateTime Now
        {
            get { return DateTime.UtcNow + LocalUtcOffset; }
        }

        static FastDateTime()
        {
            LocalUtcOffset = TimeZone.CurrentTimeZone.GetUtcOffset(DateTime.Now);
        }
    }

    //------------------------------------------------------------------------------------------------------------------

    internal static class Helper
    {
        public static MurmurHash2Unsafe MurMur = new MurmurHash2Unsafe();
        public static int CompareMemCmp(byte[] left, byte[] right)
        {
            int c = left.Length;
            if (c > right.Length)
                c = right.Length;
            return memcmp(left, right, c);
        }

        [DllImport("msvcrt.dll", CallingConvention = CallingConvention.Cdecl)]
        private static extern int memcmp(byte[] arr1, byte[] arr2, int cnt);

        internal static unsafe int ToInt32(byte[] value, int startIndex, bool reverse)
        {
            if (reverse)
            {
                byte[] b = new byte[4];
                Buffer.BlockCopy(value, startIndex, b, 0, 4);
                Array.Reverse(b);
                return ToInt32(b, 0);
            }

            return ToInt32(value, startIndex);
        }

        internal static unsafe int ToInt32(byte[] value, int startIndex)
        {
            fixed (byte* numRef = &(value[startIndex]))
            {
                return *((int*)numRef);
            }
        }

        internal static unsafe long ToInt64(byte[] value, int startIndex, bool reverse)
        {
            if (reverse)
            {
                byte[] b = new byte[8];
                Buffer.BlockCopy(value, startIndex, b, 0, 8);
                Array.Reverse(b);
                return ToInt64(b, 0);
            }
            return ToInt64(value, startIndex);
        }

        internal static unsafe long ToInt64(byte[] value, int startIndex)
        {
            fixed (byte* numRef = &(value[startIndex]))
            {
                return *(((long*)numRef));
            }
        }

        internal static unsafe short ToInt16(byte[] value, int startIndex, bool reverse)
        {
            if (reverse)
            {
                byte[] b = new byte[2];
                Buffer.BlockCopy(value, startIndex, b, 0, 2);
                Array.Reverse(b);
                return ToInt16(b, 0);
            }
            return ToInt16(value, startIndex);
        }

        internal static unsafe short ToInt16(byte[] value, int startIndex)
        {
            fixed (byte* numRef = &(value[startIndex]))
            {
                return *(((short*)numRef));
            }
        }

        internal static unsafe byte[] GetBytes(long num, bool reverse)
        {
            byte[] buffer = new byte[8];
            fixed (byte* numRef = buffer)
            {
                *((long*)numRef) = num;
            }
            if (reverse)
                Array.Reverse(buffer);
            return buffer;
        }

        public static unsafe byte[] GetBytes(int num, bool reverse)
        {
            byte[] buffer = new byte[4];
            fixed (byte* numRef = buffer)
            {
                *((int*)numRef) = num;
            }
            if (reverse)
                Array.Reverse(buffer);
            return buffer;
        }

        internal static byte[] GetBytes(string s)
        {
            return Encoding.UTF8.GetBytes(s);
        }

        internal static string GetString(byte[] buffer, int index, short keylength)
        {
            return Encoding.UTF8.GetString(buffer, index, keylength);
        }
    }
}
