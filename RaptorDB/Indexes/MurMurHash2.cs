using System;
using System.Collections.Generic;
using System.Text;

namespace RaptorDB
{
    internal class Hash<T> : IIndex<T> where T : IComparable<T> , IGetBytes<T>
    {
        private int _BucketCount = 10007;
        private int _BucketItems = 200;
        private List<int> _bucketPointers = new List<int>();
        MurmurHash2Unsafe _hash = new MurmurHash2Unsafe();
        private SafeDictionary<int, Bucket<T>> _CachedBuckets = new SafeDictionary<int, Bucket<T>>(Global.MaxItemsBeforeIndexing);
        private IndexFile<T> _indexfile;
        private bool _allowDuplicates;
        private bool _InMemory = false;
        ILog log = LogManager.GetLogger(typeof(Hash<T>));

        public Hash(string indexfilename, byte maxkeysize, short nodeSize, bool allowDuplicates, int bucketcount)
        {
            _BucketCount = bucketcount;
            _BucketItems = nodeSize;
            _allowDuplicates = allowDuplicates;
            // fill buckets with blanks
            for (int i = 0; i < _BucketCount; i++)
                _bucketPointers.Add(-1);

            _indexfile = new IndexFile<T>(indexfilename, maxkeysize, nodeSize, bucketcount, INDEXTYPE.HASH);

            _BucketItems = _indexfile._PageNodeCount;

            // load data from index file
            _bucketPointers = _indexfile.GetBucketList();
        }

        #region [   I I N D E X   ]

        public bool InMemory
        {
            get
            {
                return _InMemory;
            }
            set
            {
                _InMemory = value;
            }
        }

        public bool Get(T key, out int offset)
        {
            offset = -1;
            Bucket<T> b = FindBucket(key);
            return SearchBucket(b, key, ref offset);
        }

        public void Set(T key, int offset)
        {
            Bucket<T> b = FindBucket(key);
            b = SetBucket(key, offset, b);
        }

        public void Commit()
        {
            if (_InMemory == false)
                SaveIndex();
        }

        public void Shutdown()
        {
            //Commit();
            SaveIndex();
            _indexfile.Shutdown();
        }

        public IEnumerable<int> Enumerate(T fromkey, int start, int count)
        {
            throw new NotImplementedException();
        }

        public long Count()
        {
            return _indexfile.CountBuckets();
        }

        public IEnumerable<int> GetDuplicates(T key)
        {
            if (_allowDuplicates)
            {
                Bucket<T> b = FindBucket(key);
                bool found = false;
                int pos = FindPointerOrLower(b, key, out found);

                if (found)
                {
                    int dp = b.Pointers[pos].DuplicatesRecNo;
                    if (dp != -1)
                    {
                        return _indexfile.GetDuplicatesRecordNumbers(dp);
                    }
                }
            }
            return new List<int>();
        }

        public void SaveIndex()
        {
            if (_indexfile.Commit(_CachedBuckets.GetList()))
            {
                _CachedBuckets = new SafeDictionary<int, Bucket<T>>(Global.MaxItemsBeforeIndexing);
            }
            _indexfile.SaveBucketList(_bucketPointers);
        }
        #endregion

        #region [   P R I V A T E   M E T H O D S   ]

        private Bucket<T> SetBucket(T key, int offset, Bucket<T> b)
        {
            bool found = false;
            int pos = FindPointerOrLower(b, key, out found);

            if (found)
            {
                KeyPointer<T> p = b.Pointers[pos];
                int v = p.RecordNum;

                // duplicate found     
                if (v != offset)
                {
                    p.RecordNum = offset;
                    if (_allowDuplicates)
                    {
                        if (p.DuplicatesRecNo == -1)
                            p.DuplicatesRecNo = _indexfile.GetBitmapDuplaicateFreeRecordNumber();

                        _indexfile.SetBitmapDuplicate(p.DuplicatesRecNo, v);
                    }
                    DirtyBucket(b);
                }
            }
            else
            {
                if (b.Pointers.Count < _BucketItems)
                {
                    KeyPointer<T> k = new KeyPointer<T>(key, offset);
                    pos++;
                    if (pos < b.Pointers.Count)
                        b.Pointers.Insert(pos, k);
                    else
                        b.Pointers.Add(k);
                    DirtyBucket(b);
                }
                else
                {
                    int p = b.NextPageNumber;
                    if (p != -1)
                    {
                        b = LoadBucket(p);
                        SetBucket(key, offset, b);
                    }
                    else
                    {
                        Bucket<T> newb = new Bucket<T>(_indexfile.GetNewPageNumber());
                        b.NextPageNumber = newb.DiskPageNumber;
                        DirtyBucket(b);
                        SetBucket(key, offset, newb);
                    }
                }
            }
            return b;
        }

        private bool SearchBucket(Bucket<T> b, T key, ref int offset)
        {
            bool found = false;
            int pos = FindPointerOrLower(b, key, out found);
            if (found)
            {
                KeyPointer<T> k = b.Pointers[pos];
                offset = k.RecordNum;
                return true;
            }
            else
            {
                if (b.NextPageNumber != -1)
                {
                    b = LoadBucket(b.NextPageNumber);
                    return SearchBucket(b, key, ref offset);
                }
            }
            return false;
        }

        private void DirtyBucket(Bucket<T> b)
        {
            if (b.isDirty)
                return;

            b.isDirty = true;
            Bucket<T> bb = null;
            if (_CachedBuckets.TryGetValue(b.DiskPageNumber, out bb) == false)
                _CachedBuckets.Add(b.DiskPageNumber, b);
        }

        private int FindPointerOrLower(Bucket<T> b, T key, out bool found)
        {
            found = false;
            if (b.Pointers.Count == 0)
                return 0;
            // binary search
            int lastlower = -1;
            int first = 0;
            int last = b.Pointers.Count - 1;
            int mid = 0;
            while (first <= last)
            {
                mid = (first + last) >> 1;
                KeyPointer<T> k = b.Pointers[mid];
                int compare = k.Key.CompareTo(key);
                if (compare < 0)
                {
                    lastlower = mid;
                    first = mid + 1;
                }
                if (compare == 0)
                {
                    found = true;
                    return mid;
                }
                if (compare > 0)
                {
                    last = mid - 1;
                }
            }

            return lastlower;
        }

        private Bucket<T> FindBucket(T key)
        {
            Bucket<T> b;
            uint h = _hash.Hash(key.GetBytes());

            int bucketNumber = (int)(h % _BucketCount);

            int pointer = _bucketPointers[bucketNumber];
            if (pointer == -1)
            {
                // new bucket
                b = CreateBucket(bucketNumber);
                _bucketPointers[bucketNumber] = b.DiskPageNumber;
                _CachedBuckets.Add(b.DiskPageNumber, b);
            }
            else
                b = LoadBucket(pointer);

            return b;
        }

        private Bucket<T> LoadBucket(int pagenumber)
        {
            Bucket<T> b;
            // try cache first
            if (_CachedBuckets.TryGetValue(pagenumber, out b))
                return b;
            // else load from disk and put in cache
            b = _indexfile.LoadBucketFromPage(pagenumber);
            _CachedBuckets.Add(pagenumber, b);

            return b;
        }

        private Bucket<T> CreateBucket(int bucketNumber)
        {
            Bucket<T> b = new Bucket<T>(_indexfile.GetNewPageNumber());
            b.BucketNumber = bucketNumber;
            // get next free indexfile pointer offset

            return b;
        }
        #endregion
    }

    public class MurmurHash2Unsafe
    {
        public UInt32 Hash(Byte[] data)
        {
            return Hash(data, 0xc58f1a7b);
        }
        const UInt32 m = 0x5bd1e995;
        const Int32 r = 24;

        public unsafe UInt32 Hash(Byte[] data, UInt32 seed)
        {
            Int32 length = data.Length;
            if (length == 0)
                return 0;
            UInt32 h = seed ^ (UInt32)length;
            Int32 remainingBytes = length & 3; // mod 4
            Int32 numberOfLoops = length >> 2; // div 4
            fixed (byte* firstByte = &(data[0]))
            {
                UInt32* realData = (UInt32*)firstByte;
                while (numberOfLoops != 0)
                {
                    UInt32 k = *realData;
                    k *= m;
                    k ^= k >> r;
                    k *= m;

                    h *= m;
                    h ^= k;
                    numberOfLoops--;
                    realData++;
                }
                switch (remainingBytes)
                {
                    case 3:
                        h ^= (UInt16)(*realData);
                        h ^= ((UInt32)(*(((Byte*)(realData)) + 2))) << 16;
                        h *= m;
                        break;
                    case 2:
                        h ^= (UInt16)(*realData);
                        h *= m;
                        break;
                    case 1:
                        h ^= *((Byte*)realData);
                        h *= m;
                        break;
                    default:
                        break;
                }
            }

            // Do a few final mixes of the hash to ensure the last few
            // bytes are well-incorporated.

            h ^= h >> 13;
            h *= m;
            h ^= h >> 15;

            return h;
        }
    }
}
