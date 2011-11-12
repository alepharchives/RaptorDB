using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.IO;

namespace RaptorDB
{
    public class RaptorDBString : RaptorDBbase
    {
        private bool _caseSensitive = false;

        public RaptorDBString(string filename, bool caseSensitive)
            : base(filename)
        {
            _caseSensitive = caseSensitive;
        }

        public void Set(string key, string val)
        {
            this.Set(key, Encoding.Unicode.GetBytes(val));
        }

        public void Set(string key, byte[] val)
        {
            string str = (_caseSensitive ? key : key.ToLower());
            byte[] ustr = Encoding.Unicode.GetBytes(str);

            base.Set(ustr, val);
        }

        public bool Get(string key, out string val)
        {
            byte[] bval = null;
            val = null;

            if (this.Get(key, out bval))
            {
                val = Encoding.Unicode.GetString(bval);
                return true;
            }
            return false;
        }

        public bool Get(string key, out byte[] val)
        {
            string str = (_caseSensitive ? key : key.ToLower());
            byte[] ustr = Encoding.Unicode.GetBytes(str);

            return base.Get(ustr, out val);
        }
    }

    //----------------------------------------------------------------------------------------------------

    public class RaptorDBGuid : RaptorDBbase
    {
        public RaptorDBGuid(string filename)
            : base(filename)
        {
        }

        public void Set(Guid key, string val)
        {
            this.Set(key, Encoding.Unicode.GetBytes(val));
        }

        public void Set(Guid key, byte[] val)
        {
            base.Set(key.ToByteArray(), val);
        }

        public bool Get(Guid key, out string val)
        {
            byte[] bval = null;
            val = null;

            if (this.Get(key.ToByteArray(), out bval))
            {
                val = Encoding.Unicode.GetString(bval);
                return true;
            }
            return false;
        }

        public bool Get(Guid key, out byte[] val)
        {
            return base.Get(key.ToByteArray(), out val);
        }

        public IEnumerable<int> GetDuplicates(Guid key)
        {
            return base.GetDuplicates(key.ToByteArray());
        }
    }

    //----------------------------------------------------------------------------------------------------

    public abstract class RaptorDBbase
    {
        internal RaptorDB<rdbInt> _rap;
        private MurmurHash2Unsafe _mur = new MurmurHash2Unsafe();

        public RaptorDBbase(string filename)
        {
            Global.DEFAULTNODESIZE = 1000;
            _rap = new RaptorDB<rdbInt>(filename, 4, true, INDEXTYPE.BTREE, false);
            _rap.InMemoryIndex = true;
            _rap.StartIndexerThread();
        }


        public void Set(byte[] key, byte[] val)
        {
            uint hc = _mur.Hash(key);

            MemoryStream ms = new MemoryStream();
            ms.Write(Helper.GetBytes(key.Length, false), 0, 4);
            ms.Write(key, 0, key.Length);
            ms.Write(val, 0, val.Length);

            _rap.Set(new rdbInt(hc), ms.ToArray());
        }

        public bool Get(byte[] key, out byte[] val)
        {
            uint hc = _mur.Hash(key);
            //byte[] k = Helper.GetBytes((int)hc, false);

            if (_rap.Get(new rdbInt(hc), out val))
            {
                // unpack data
                byte[] g = null;
                if (UnpackData(val, out val, out g))
                {
                    if (Helper.CompareMemCmp(key, g) != 0)
                    {
                        // if data not equal check duplicates (hash conflict)
                        List<int> ints = new List<int>(_rap.GetDuplicates(new rdbInt((int)hc)));
                        ints.Reverse();
                        foreach (int i in ints)
                        {
                            byte[] bb = _rap.FetchDuplicate(i);
                            if (UnpackData(bb, out val, out g))
                            {
                                if (Helper.CompareMemCmp(key, g) == 0)
                                    return true;
                            }
                        }
                        return false;
                    }
                    return true;
                }
            }
            return false;
        }

        public void Shutdown()
        {
            _rap.Shutdown();
        }

        public void SaveIndex()
        {
            _rap.SaveIndex();
        }

        public long Count()
        {
            return _rap.Count();
        }

        private bool UnpackData(byte[] buffer, out byte[] val, out byte[] key)
        {
            int len = Helper.ToInt32(buffer, 0, false);
            key = new byte[len];
            Buffer.BlockCopy(buffer, 4, key, 0, len);
            val = new byte[buffer.Length - 4 - len];
            Buffer.BlockCopy(buffer, 4 + len, val, 0, buffer.Length - 4 - len);

            return true;
        }

        public IEnumerable<int> GetDuplicates(byte[] key)
        {
            uint hc = _mur.Hash(key);

            return _rap.GetDuplicates(new rdbInt(hc));
        }

        public byte[] FetchDuplicate(int rec)
        {
            byte[] b = _rap.FetchDuplicate(rec);
            byte[] val;
            byte[] key;
            if (UnpackData(b, out val, out key))
            {
                return val;
            }
            return null;
        }
    }
}
