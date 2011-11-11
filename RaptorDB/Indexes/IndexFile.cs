﻿using System;
using System.Collections.Generic;
using System.Text;
using System.IO;
using System.Collections;

namespace RaptorDB
{
    internal class IndexFile<T> where T : IGetBytes<T>
    {
        FileStream _file = null;
        private byte[] _FileHeader = new byte[] {
            (byte)'M', (byte)'G', (byte)'I',
            0,               // 3 = [keysize]   max 255
            0,0,             // 4 = [node size] max 65536
            0,0,0,0,         // 6 = [root page num]
            0,               // 10 = Index file type : 0=BTREE 1=HASH
            0,0,0,0          // 11 = bucket count 
            };

        private byte[] _BlockHeader = new byte[] { 
            (byte)'P',(byte)'A',(byte)'G',(byte)'E',
            0,               // 4 = [Flag] = 0=free 1=leaf 2=root 4=revisionpage --8=bucket 16=revisionbucket
            0,0,             // 5 = [item count] 
            0,0,0,0,         // 7 = [parent page number] / [bucket number]
            0,0,0,0          // 11 = [right page number] / [next page number]
        };

        internal byte _maxKeySize;
        internal short _PageNodeCount = 50;
        private int _LastPageNumber = 0;
        private int _PageLength;
        private int _CurrentRootPage = 0;
        private int _rowSize;
        internal int _BucketCount = 11;
        internal INDEXTYPE _indexType = INDEXTYPE.BTREE;
        ILog log = LogManager.GetLogger(typeof(IndexFile<T>));
        private BitmapIndex _bitmap;
        private T _T = default(T);

        public IndexFile(string filename, byte maxKeySize, short pageNodeCount, int bucketcount, INDEXTYPE type)
        {
            _T = Activator.CreateInstance<T>();
            _maxKeySize = maxKeySize;
            _PageNodeCount = pageNodeCount;
            _rowSize = (_maxKeySize + 1 + 4 + 4);
            _BucketCount = bucketcount;
            _indexType = type;

            string path = Path.GetDirectoryName(filename);
            Directory.CreateDirectory(path);
            if (File.Exists(filename))
            {
                // if file exists open and read header
                _file = File.Open(filename, FileMode.Open, FileAccess.ReadWrite);
                ReadFileHeader();
                // compute last page number from file length 
                _PageLength = (_BlockHeader.Length + _rowSize * (_PageNodeCount));
                _LastPageNumber = (int)((_file.Length - _FileHeader.Length - bucketcount * 4) / _PageLength);
            }
            else
            {
                // else create new file
                _file = File.Open(filename, FileMode.Create, FileAccess.ReadWrite);

                _PageLength = (_BlockHeader.Length + _rowSize * (_PageNodeCount));

                CreateFileHeader();
                _LastPageNumber = (int)((_file.Length - _FileHeader.Length - bucketcount * 4) / _PageLength);
            }
            // bitmap duplicates 
            _bitmap = new BitmapIndex(Path.GetDirectoryName(filename), Path.GetFileNameWithoutExtension(filename));

            // FEATURE : if index chk exists rollback changes to index         
        }

        #region [  C o m m o n  ]
        public void SetBitmapDuplicate(int bitmaprec, int rec)
        {
            _bitmap.SetDuplicate(bitmaprec, rec);
        }

        public int GetBitmapDuplaicateFreeRecordNumber()
        {
            return _bitmap.GetFreeRecordNumber();
        }

        public IEnumerable<int> GetDuplicatesRecordNumbers(int recno)
        {
            return _bitmap.GetBitmapNoCache(recno).GetBitIndexes(true);
        }

        public long CountNodes(Node<T> root)
        {
            long c = 0;
            Node<T> n = root;
            if (n == null)
                return 0;
            while (n.isLeafPage == false)
                n = LoadNodeFromPageNumber(n.ChildPointers[0].RecordNum);
            // n = first node
            int right = n.RightPageNumber;
            c += n.Count;
            while (right != -1)
            {
                right = NodeHeaderCount(right, ref c);
                // TODO : count duplicates also?
            }
            return c;
        }

        private int NodeHeaderCount(int nextpage, ref long c)
        {
            SeekPage(nextpage);
            byte[] b = new byte[_BlockHeader.Length];
            _file.Read(b, 0, _BlockHeader.Length);

            if (b[0] == _BlockHeader[0] && b[1] == _BlockHeader[1] && b[2] == _BlockHeader[2] && b[3] == _BlockHeader[3])
            {
                short count = Helper.ToInt16(b, 5);
                int rightpage = Helper.ToInt32(b, 11);
                c += count;
                return rightpage;
            }
            return 0;
        }

        public long CountBuckets()
        {
            long c = 0;
            foreach (int l in GetBucketList())
            {
                int next = -1;
                if (l != -1)
                {
                    // TODO : count duplicates also ?
                    next = BucketHeaderCount(l, ref c);
                    while (next != -1)
                        next = BucketHeaderCount(next, ref c);
                }
            }

            return c;
        }

        private int BucketHeaderCount(int nextbucket, ref long c)
        {
            SeekPage(nextbucket);
            byte[] b = new byte[_BlockHeader.Length];
            _file.Read(b, 0, _BlockHeader.Length);

            if (b[0] == _BlockHeader[0] && b[1] == _BlockHeader[1] && b[2] == _BlockHeader[2] && b[3] == _BlockHeader[3])
            {
                short count = Helper.ToInt16(b, 5);
                int nextpage = Helper.ToInt32(b, 11);
                c += count;
                return nextpage;
            }
            return 0;
        }

        private void CreateFileHeader()
        {
            // max key size
            byte[] b = Helper.GetBytes(_maxKeySize, false);
            Buffer.BlockCopy(b, 0, _FileHeader, 3, 1);
            // page node count
            b = Helper.GetBytes(_PageNodeCount, false);
            Buffer.BlockCopy(b, 0, _FileHeader, 4, 2);
            // bucket count
            b = Helper.GetBytes(_BucketCount, false);
            Buffer.BlockCopy(b, 0, _FileHeader, 11, 4);
            if (_indexType == INDEXTYPE.HASH)
                _FileHeader[10] = 1;

            _file.Seek(0L, SeekOrigin.Begin);
            _file.Write(_FileHeader, 0, _FileHeader.Length);

            // write bucket lookup
            b = new byte[_BucketCount * 4];
            for (int i = 0; i < b.Length; i++)
                b[i] = 255;
            _file.Write(b, 0, b.Length);
            _file.Flush();

            if (_indexType == INDEXTYPE.BTREE)
            {
                Node<T> n = new Node<T>(3, -1, new List<KeyPointer<T>>(), 0, -1);
                SaveNode(n);
            }
        }

        private void WriteHeader(long rootpage)
        {
            byte[] b = Helper.GetBytes(rootpage, false);
            Buffer.BlockCopy(b, 0, _FileHeader, 6, b.Length);
            _file.Seek(0L, SeekOrigin.Begin);
            _file.Write(_FileHeader, 0, _FileHeader.Length);
            _file.Flush();
        }

        private bool ReadFileHeader()
        {
            _file.Seek(0L, SeekOrigin.Begin);
            byte[] b = new byte[_FileHeader.Length];
            _file.Read(b, 0, _FileHeader.Length);

            if (b[0] == _FileHeader[0] && b[1] == _FileHeader[1] && b[2] == _FileHeader[2])
            {
                byte maxks = b[3];
                short nodes = Helper.ToInt16(b, 4);
                int root = Helper.ToInt32(b, 6);
                _maxKeySize = maxks;
                _PageNodeCount = nodes;
                _CurrentRootPage = root;
                _FileHeader = b;
                if (b[10] == (int)_indexType)
                    return true;
                else
                    throw new Exception("Index type does not match file index type");
            }

            return false;
        }

        public int GetNewPageNumber()
        {
            return _LastPageNumber++;
        }

        private void SeekPage(int pnum)
        {
            long offset = _FileHeader.Length + _BucketCount * 4;
            offset += (long)pnum * _PageLength;
            if (offset > _file.Length)
                throw new Exception("file seek out of bounds =" + offset + " file len = " + _file.Length);
            _file.Seek(offset, SeekOrigin.Begin);
        }

        public void Shutdown()
        {
            log.Debug("Shutdown IndexFile");
            if (_file != null)
            {
                _file.Flush();
                _file.Close();
            }
            _file = null;
            _bitmap.Shutdown();
        }

        #endregion

        #region [  N O D E S  ]
        public Node<T> GetRoot()
        {
            return LoadNodeFromPageNumber(_CurrentRootPage);
        }

        public bool Commit(ICollection<KeyValuePair<int, Node<T>>> nodes)
        {
            foreach (KeyValuePair<int, Node<T>> n in nodes)
            {
                if (n.Value.isDirty)
                {
                    SaveNode(n.Value);
                    if (n.Value.isRootPage)
                    {
                        WriteHeader(n.Value.DiskPageNumber);
                        _CurrentRootPage = n.Value.DiskPageNumber;
                    }
                }
            }
            _file.Flush();
            // free memory config here
            _bitmap.Commit(Global.FreeMemoryOnCommit);

            return true;
        }

        private void SaveNode(Node<T> node)
        {
            int pnum = node.DiskPageNumber;
            if (pnum > _LastPageNumber)
                throw new Exception("should not be here: page out of bounds");

            SeekPage(pnum);
            byte[] page = new byte[_PageLength];
            Buffer.BlockCopy(_BlockHeader, 0, page, 0, _BlockHeader.Length);
            // node type
            if (node.isLeafPage)
                page[4] = 1;
            if (node.isRootPage)
                page[4] += 2;
            if (node.isDuplicatePage)
                page[4] += 4;

            // node count
            byte[] b = Helper.GetBytes(node.Count, false);
            Buffer.BlockCopy(b, 0, page, 5, b.Length);
            // node parent
            b = Helper.GetBytes(node.ParentPageNumber, false);
            Buffer.BlockCopy(b, 0, page, 7, b.Length);
            // right page number
            b = Helper.GetBytes(node.RightPageNumber, false);
            Buffer.BlockCopy(b, 0, page, 11, b.Length);

            int index = _BlockHeader.Length;
            // node children
            for (int i = 0; i < node.Count; i++)
            {
                KeyPointer<T> kp = node.ChildPointers[i];
                int idx = index + _rowSize * i;

                // key bytes
                byte[] kk = node.ChildPointers[i].Key.GetBytes();
                byte size = (byte)kk.Length;
                if (size > _maxKeySize)
                    size = _maxKeySize;
                // key size = 1 byte
                page[idx] = size;
                Buffer.BlockCopy(kk, 0, page, idx + 1, page[idx]);
                // offset = 4 bytes
                b = Helper.GetBytes(kp.RecordNum, false);
                Buffer.BlockCopy(b, 0, page, idx + 1 + _maxKeySize, b.Length);
                // duplicatepage = 4 bytes
                b = Helper.GetBytes(kp.DuplicatesRecNo, false);
                Buffer.BlockCopy(b, 0, page, idx + 1 + _maxKeySize + 4, b.Length);
            }
            _file.Write(page, 0, page.Length);
            //_file.Flush();
        }

        public Node<T> LoadNodeFromPageNumber(int number)
        {
            SeekPage(number);
            byte[] b = new byte[_PageLength];
            _file.Read(b, 0, _PageLength);

            if (b[0] == _BlockHeader[0] && b[1] == _BlockHeader[1] && b[2] == _BlockHeader[2] && b[3] == _BlockHeader[3])
            {
                // create node here
                int parentpage = Helper.ToInt32(b, 7);
                List<KeyPointer<T>> list = new List<KeyPointer<T>>();
                short count = Helper.ToInt16(b, 5);
                if (count > _PageNodeCount)
                    throw new Exception("Count > node size");
                int rightpage = Helper.ToInt32(b, 11);
                int index = _BlockHeader.Length;

                for (int i = 0; i < count; i++)
                {
                    int idx = index + _rowSize * i;
                    byte ks = b[idx];
                    T key = _T.GetObject(b, idx + 1, ks);
                    int offset = Helper.ToInt32(b, idx + 1 + _maxKeySize);
                    int duppage = Helper.ToInt32(b, idx + 1 + _maxKeySize + 4);
                    KeyPointer<T> kp = new KeyPointer<T>(key, offset, duppage);
                    list.Add(kp);
                }
                Node<T> n = new Node<T>(b[4], parentpage, list, number, rightpage);
                return n;
            }
            else
                throw new Exception("Page read error");
        }
        #endregion

        #region [  B U C K E T S  ]
        public List<int> GetBucketList()
        {
            _file.Seek(_FileHeader.Length, SeekOrigin.Begin);
            byte[] b = new byte[_BucketCount * 4];
            _file.Read(b, 0, b.Length);
            List<int> list = new List<int>();
            for (int i = 0; i < _BucketCount; i++)
            {
                int l = Helper.ToInt32(b, i * 4);
                list.Add(l);
            }

            return list;
        }

        public void SaveBucketList(List<int> list)
        {
            byte[] b = new byte[list.Count * 4];
            int idx = 0;
            foreach (int l in list)
            {
                byte[] bb = Helper.GetBytes(l, false);
                Buffer.BlockCopy(bb, 0, b, idx, bb.Length);
                idx += bb.Length;
            }
            _file.Seek(_FileHeader.Length, SeekOrigin.Begin);
            _file.Write(b, 0, b.Length);
        }

        public void SaveBucket(Bucket<T> bucket)
        {
            int pnum = bucket.DiskPageNumber;
            if (pnum > _LastPageNumber)
                throw new Exception("should not be here: page out of bounds");

            SeekPage(pnum);
            byte[] page = new byte[_PageLength];
            Buffer.BlockCopy(_BlockHeader, 0, page, 0, _BlockHeader.Length);
            // bucket type
            if (bucket.isBucket)
                page[4] = 8;
            if (bucket.isOverflow)
                page[4] += 16;


            // bucket count
            byte[] b = Helper.GetBytes(bucket.Count, false);
            Buffer.BlockCopy(b, 0, page, 5, b.Length);
            // bucket number
            b = Helper.GetBytes(bucket.BucketNumber, false);
            Buffer.BlockCopy(b, 0, page, 7, b.Length);
            // next page number
            b = Helper.GetBytes(bucket.NextPageNumber, false);
            Buffer.BlockCopy(b, 0, page, 11, b.Length);

            int index = _BlockHeader.Length;

            // bucket children
            for (int i = 0; i < bucket.Count; i++)
            {
                KeyPointer<T> kp = bucket.Pointers[i];
                int idx = index + _rowSize * i;
                // key bytes
                byte[] kk = bucket.Pointers[i].Key.GetBytes();
                byte size = (byte)kk.Length;
                if (size > _maxKeySize)
                    size = _maxKeySize;
                // key size = 1 byte
                page[idx] = size;
                Buffer.BlockCopy(kk, 0, page, idx + 1, page[idx]);
                // offset = 4 bytes
                b = Helper.GetBytes(kp.RecordNum, false);
                Buffer.BlockCopy(b, 0, page, idx + 1 + _maxKeySize, b.Length);
                // duplicatepage = 4 bytes
                b = Helper.GetBytes(kp.DuplicatesRecNo, false);
                Buffer.BlockCopy(b, 0, page, idx + 1 + _maxKeySize + 4, b.Length);
            }

            _file.Write(page, 0, page.Length);
        }

        public Bucket<T> LoadBucketFromPage(int page)
        {
            SeekPage(page);
            byte[] b = new byte[_PageLength];
            _file.Read(b, 0, _PageLength);

            if (b[0] == _BlockHeader[0] && b[1] == _BlockHeader[1] && b[2] == _BlockHeader[2] && b[3] == _BlockHeader[3])
            {
                // create node here
                int bucketnum = Helper.ToInt32(b, 7);
                List<KeyPointer<T>> list = new List<KeyPointer<T>>();
                short count = Helper.ToInt16(b, 5);
                if (count > _PageNodeCount)
                    throw new Exception("Count > node size");
                int nextpage = Helper.ToInt32(b, 11);
                int index = _BlockHeader.Length;

                for (int i = 0; i < count; i++)
                {
                    int idx = index + _rowSize * i;
                    byte ks = b[idx];
                    T key = _T.GetObject(b, idx + 1, ks);
                    int offset = Helper.ToInt32(b, idx + 1 + _maxKeySize);
                    int duppage = Helper.ToInt32(b, idx + 1 + _maxKeySize + 4);

                    KeyPointer<T> kp = new KeyPointer<T>(key, offset, duppage);
                    list.Add(kp);
                }

                Bucket<T> n = new Bucket<T>(b[4], bucketnum, list, page, nextpage);
                return n;
            }
            else
                throw new Exception("Page read error");
        }

        public bool Commit(ICollection<KeyValuePair<int, Bucket<T>>> buckets)
        {
            foreach (KeyValuePair<int, Bucket<T>> n in buckets)
            {
                if (n.Value.isDirty)
                    SaveBucket(n.Value);
            }
            _file.Flush();
            // free memory config
            _bitmap.Commit(Global.FreeMemoryOnCommit);
            return true;
        }
        #endregion
    }
}