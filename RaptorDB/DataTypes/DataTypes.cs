using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace RaptorDB
{
    internal interface IGetBytes<T>
    {
        byte[] GetBytes(T obj);
        T GetObject(byte[] buffer, int offset, int count);
    }

    internal class RDBDataType<T>
    {
        public static IGetBytes<T> ByteHandler()
        {
            Type type = typeof(T);

            if (type == typeof(int))
                return (IGetBytes<T>)new inthandler<T>();

            else if (type == typeof(uint))
                return (IGetBytes<T>)new uinthandler<T>();

            else if (type == typeof(long))
                return (IGetBytes<T>)new longhandler<T>();

            else if (type == typeof(Guid))
                return (IGetBytes<T>)new guidhandler<T>();

            else if (type == typeof(string))
                return (IGetBytes<T>)new stringhandler<T>();

            return null;
        }

        public static byte GetByteSize(byte keysize)
        {
            byte size = 4;
            Type t = typeof(T);

            if (t == typeof(int))
                size = 4;
            if (t == typeof(uint))
                size = 4;
            if (t == typeof(long))
                size = 8;
            if (t == typeof(Guid))
                size = 16;
            if (t == typeof(string))
                size = keysize;

            return size;
        }
    }

    internal class stringhandler<T> : IGetBytes<string>
    {
        public byte[] GetBytes(string obj)
        {
            return Helper.GetBytes(obj);
        }

        public string GetObject(byte[] buffer, int offset, int count)
        {
            return Helper.GetString(buffer, offset, (short)count);
        }
    }

    internal class inthandler<T> : IGetBytes<int>
    {
        public byte[] GetBytes(int obj)
        {
            return Helper.GetBytes(obj, false);
        }

        public int GetObject(byte[] buffer, int offset, int count)
        {
            return Helper.ToInt32(buffer, offset);
        }
    }

    internal class uinthandler<T> : IGetBytes<uint>
    {
        public byte[] GetBytes(uint obj)
        {
            return Helper.GetBytes(obj, false);
        }

        public uint GetObject(byte[] buffer, int offset, int count)
        {
            return (uint)Helper.ToInt32(buffer, offset);
        }
    }

    internal class longhandler<T> : IGetBytes<long>
    {
        public byte[] GetBytes(long obj)
        {
            return Helper.GetBytes(obj, false);
        }

        public long GetObject(byte[] buffer, int offset, int count)
        {
            return Helper.ToInt64(buffer, offset);
        }
    }

    internal class guidhandler<T> : IGetBytes<Guid>
    {
        public byte[] GetBytes(Guid obj)
        {
            return obj.ToByteArray();
        }

        public Guid GetObject(byte[] buffer, int offset, int count)
        {
            byte[] b = new byte[16];
            Buffer.BlockCopy(buffer, offset, b, 0, 16);
            return new Guid(b);
        }
    }
}