using System;
using System.Collections.Generic;
using System.Runtime.InteropServices;
using System.Text;

namespace RaptorDB
{
    public class SafeDictionary<TKey, TValue>
    {
        private readonly object _Padlock = new object();
        private readonly Dictionary<TKey, TValue> _Dictionary = new Dictionary<TKey, TValue>();

        public SafeDictionary(int capacity)
        {
            _Dictionary = new Dictionary<TKey, TValue>(capacity);
        }

        public SafeDictionary(int capacity, IEqualityComparer<TKey> comp)
        {
            _Dictionary = new Dictionary<TKey, TValue>(capacity, comp);
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
                _Dictionary[key] = value;
            }
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
            {
                if (_Dictionary.ContainsKey(key) == false)
                    _Dictionary.Add(key, value);
                else
                    _Dictionary[key] = value;
            }
        }

        public void Remove(TKey key)
        {
            lock (_Padlock)
            {
                _Dictionary.Remove(key);
            }
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

    public static class Helper
    {
        public static MurmurHash2Unsafe MurMur = new MurmurHash2Unsafe();

        //[StructLayout(LayoutKind.Sequential, CharSet = CharSet.Auto)]
        //private class MEMORYSTATUSEX
        //{
        //    public uint dwLength;
        //    public uint dwMemoryLoad;
        //    public ulong ullTotalPhys;
        //    public ulong ullAvailPhys;
        //    public ulong ullTotalPageFile;
        //    public ulong ullAvailPageFile;
        //    public ulong ullTotalVirtual;
        //    public ulong ullAvailVirtual;
        //    public ulong ullAvailExtendedVirtual;
        //    public MEMORYSTATUSEX()
        //    {
        //        this.dwLength = (uint)Marshal.SizeOf(typeof(MEMORYSTATUSEX));
        //    }
        //}

        //[return: MarshalAs(UnmanagedType.Bool)]
        //[DllImport("kernel32.dll", CharSet = CharSet.Auto, SetLastError = true)]
        //private static extern bool GlobalMemoryStatusEx([In, Out] MEMORYSTATUSEX lpBuffer);

        //internal static int GetFreeMemory()
        //{
        //    ulong installedMemory = 0;
        //    MEMORYSTATUSEX memStatus = new MEMORYSTATUSEX();
        //    if (GlobalMemoryStatusEx(memStatus))
        //    {
        //        installedMemory = memStatus.ullAvailPhys >> 20;
        //    }
        //    return (int)installedMemory;
        //}

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

        internal unsafe static int CompareUnSafe(byte[] left, byte[] right)
        {
            int c = left.Length;
            if (c > right.Length)
                c = right.Length;
            fixed (byte* p1 = left, p2 = right)
            {
                for (int i = 0; i < c; i++)
                {
                    int a = p1[i];
                    int b = p2[i];
                    if (a != b)
                    {
                        return a - b;
                    }
                }
            }
            return left.Length - right.Length;
        }

        public static int CompareSafe(byte[] left, byte[] right)
        {
            int c = left.Length;
            if (c > right.Length)
                c = right.Length;
            for (int i = 0; i < c; i++)
            {
                int a = left[i];
                int b = right[i];
                if (a != b)
                {
                    return a - b;
                }
            }
            return left.Length - right.Length;
        }

        public static int CompareMemCmp(byte[] left, byte[] right)
        {
            int c = left.Length;
            if (c > right.Length)
                c = right.Length;
            return memcmp(left, right, c);
        }

        [DllImport("msvcrt.dll", CallingConvention = CallingConvention.Cdecl)]
        private static extern int memcmp(byte[] arr1, byte[] arr2, int cnt);

        internal static byte[] GetBytes(string s)
        {
            return Encoding.UTF8.GetBytes(s);

            //byte[] b = new byte[s.Length];
            //char[] cc = s.ToCharArray();
            //int l = cc.Length;
            //for (int i = 0; i < l; i++)//foreach (char c in s)
            //    b[i] = (byte)cc[i];// c;
            //return b;
        }

        internal static string GetString(byte[] bytes)
        {
            return Encoding.UTF8.GetString(bytes);
            //char[] cc = new char[bytes.Length];
            //int i=0;
            //foreach (byte b in bytes)
            //    cc[i++] = (char)b;

            //return new string(cc);
        }

        internal static string GetString(byte[] buffer, int index, short keylength)
        {
            return Encoding.UTF8.GetString(buffer, index, keylength);
            //char[] cc = new char[keylength];

            //for (int i = 0; i < keylength; i++)
            //    cc[i] = (char)buffer[index + i];

            //return new string(cc);
        }
    }
}
