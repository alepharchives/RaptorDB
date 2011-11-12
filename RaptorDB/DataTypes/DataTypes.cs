using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace RaptorDB
{
    public interface IRDBDataType<T> : IComparable<T>, IGetBytes<T>, IEqualityComparer<T>, IEquatable<T>
    {
    }
    
    //------------------------------------------------------------------------------------------------------------------

    public interface IGetBytes<T>
    {
        byte[] GetBytes();
        T GetObject(byte[] buffer, int offset, int count);
    }

    //----------------------------------------------------------------------------------------------------

    public class rdbByteArray : IRDBDataType<rdbByteArray>
    {
        public rdbByteArray()
        {

        }

        public rdbByteArray(byte[] key)
        {
            val = key;
        }
        public byte[] val;

        public int CompareTo(rdbByteArray obj)
        {
            return Helper.CompareUnSafe(val, obj.val);
        }

        public byte[] GetBytes()
        {
            return val;
        }

        public bool Equals(rdbByteArray a, rdbByteArray b)
        {
            if (a == null || b == null)
                return a == b;
            if (a.val.Length != b.val.Length)
                return false;
            int i = Helper.CompareUnSafe(a.val, b.val);
            return i==0?true:false;
        }

        public int GetHashCode(rdbByteArray obj)
        {
            if (obj == null)
                throw new ArgumentNullException();
            int iHash = 0;
            for (int i = 0; i < obj.val.Length; ++i)
                iHash ^= (obj.val[i] << ((0x03 & i) << 3));
            return iHash;
        }

        public bool Equals(rdbByteArray other)
        {
            if (other == null)
                return false;
            if (this.val.Length != other.val.Length)
                return false;
            int i = Helper.CompareUnSafe(this.val, other.val);
            return i == 0 ? true : false;
        }

        public rdbByteArray GetObject(byte[] buffer, int offset, int count)
        {
            byte[] b = new byte[count];
            Buffer.BlockCopy(buffer, offset, b, 0, count);
            return new rdbByteArray(b);
        }
    }

    //----------------------------------------------------------------------------------------------------

    public class rdbInt : IRDBDataType<rdbInt>
    {
        public rdbInt()
        {

        }
        public rdbInt(int i)
        {
            _i = i;
        }
        public rdbInt(uint i)
        {
            _i = (int)i;
        }
        private int _i;

        public int CompareTo(rdbInt other)
        {
            return _i.CompareTo(other._i);
        }

        public byte[] GetBytes()
        {
            return Helper.GetBytes(_i, false);
        }

        public bool Equals(rdbInt x, rdbInt y)
        {
            return x._i == y._i;
        }

        public int GetHashCode(rdbInt obj)
        {
            return obj._i;
        }

        public bool Equals(rdbInt other)
        {
            return this._i == other._i;
        }

        public rdbInt GetObject(byte[] buffer, int offset, int count)
        {
            return new rdbInt(Helper.ToInt32(buffer, offset));
        }
    }

    //----------------------------------------------------------------------------------------------------

    public class rdbLong : IRDBDataType<rdbLong>
    {
        public rdbLong()
        {

        }
        public rdbLong(long i)
        {
            _i = i;
        }
        private long _i;

        public int CompareTo(rdbLong other)
        {
            return _i.CompareTo(other._i);
        }

        public byte[] GetBytes()
        {
            return Helper.GetBytes(_i, false);
        }

        public bool Equals(rdbLong x, rdbLong y)
        {
            return x._i == y._i;
        }

        public int GetHashCode(rdbLong obj)
        {
            return obj._i.GetHashCode();
        }

        public bool Equals(rdbLong other)
        {
            return this._i == other._i;
        }

        public rdbLong GetObject(byte[] buffer, int offset, int count)
        {
            return new rdbLong(Helper.ToInt64(buffer, offset));
        }
    }
}
