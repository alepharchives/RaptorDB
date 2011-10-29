using System;
using System.Collections.Generic;
using System.Text;
using System.IO;

namespace RaptorDB
{
    internal class BitmapIndex
    {
        public BitmapIndex(string path, string filename)
        {
            _FileName = Path.GetFileNameWithoutExtension(filename);
            _Path = path;
            if (_Path.EndsWith("\\") == false) _Path += "\\";

            _recordFileRead = new FileStream(_Path + _FileName + _recExt, FileMode.OpenOrCreate, FileAccess.ReadWrite, FileShare.ReadWrite);
            _recordFileWrite = new FileStream(_Path + _FileName + _recExt, FileMode.OpenOrCreate, FileAccess.ReadWrite, FileShare.ReadWrite);
            _bitmapFileWrite = new FileStream(_Path + _FileName + _bmpExt, FileMode.OpenOrCreate, FileAccess.ReadWrite, FileShare.ReadWrite);
            _bitmapFileRead = new FileStream(_Path + _FileName + _bmpExt, FileMode.OpenOrCreate, FileAccess.ReadWrite, FileShare.ReadWrite);

            _bitmapFileWrite.Seek(0L, SeekOrigin.End);
            _lastBitmapOffset = _bitmapFileWrite.Length;
            _lastRecordNumber = (int)(_recordFileRead.Length / 8);
        }

        private string _recExt = ".mgbmr";
        private string _bmpExt = ".mgbmp";
        private string _FileName = "";
        private string _Path = "";
        private FileStream _bitmapFileWrite;
        private FileStream _bitmapFileRead;
        private FileStream _recordFileRead;
        private FileStream _recordFileWrite;
        private long _lastBitmapOffset = 0;
        private int _lastRecordNumber = 0;
        private object _lock = new object();
        private SafeDictionary<int, WAHBitArray> _cache = new SafeDictionary<int, WAHBitArray>();

        #region [  P U B L I C  ]
        public void Shutdown()
        {
            Flush();

            _recordFileRead.Close();
            _recordFileWrite.Close();
            _bitmapFileWrite.Close();
            _bitmapFileRead.Close();
        }

        public void Flush()
        {
            _recordFileWrite.Flush();
            _bitmapFileWrite.Flush();
        }

        public int GetFreeRecordNumber()
        {
            int i = _lastRecordNumber++;

            _cache.Add(i, new WAHBitArray());
            return i;
        }

        public void Commit(bool freeMemory)
        {
            foreach (KeyValuePair<int, WAHBitArray> kv in _cache)
            {
                if (kv.Value.isDirty)
                {
                    SaveBitmap(kv.Key, kv.Value);
                    kv.Value.FreeMemory();
                    kv.Value.isDirty = false;
                }
            }
            Flush();
            if (freeMemory)
            {
                //Console.WriteLine("clearing cache");
                _cache = new SafeDictionary<int, WAHBitArray>();
            }
        }

        public void SetDuplicate(int bitmaprecno, int record)
        {
            WAHBitArray ba = null;

            ba = GetBitmap(bitmaprecno);

            ba.Set(record, true);
        }

        public WAHBitArray GetBitmap(int recno)
        {
            return internalGetBitmap(recno, true);
        }

        public WAHBitArray GetBitmapNoCache(int recno)
        {
            return internalGetBitmap(recno, false);
        }

        //public void OptimizeIndex()
        //{
            // FIX : optimize index here

            //lock (_lock)
            //{
            //    _internalOP = true;
            //    _lastBitmapOffset = 0;
            //    _bitmapFile.Flush();
            //    _bitmapFile.Close();
            //    // compact bitmap index file to new file
            //    _bitmapFile = new FileStream(_Path + _FileName + ".bitmap$", FileMode.OpenOrCreate, FileAccess.ReadWrite, FileShare.ReadWrite);
            //    MemoryStream ms = new MemoryStream();
            //    BinaryWriter bw = new BinaryWriter(ms, Encoding.UTF8);
            //    // save words and bitmaps
            //    using (FileStream words = new FileStream(_Path + _FileName + ".words", FileMode.Create))
            //    {
            //        foreach (KeyValuePair<string, Cache> kv in _index)
            //        {
            //            bw.Write(kv.Key);
            //            uint[] ar = LoadBitmap(kv.Value.FileOffset);
            //            long offset = SaveBitmap(ar, ar.Length, 0);
            //            kv.Value.FileOffset = offset;
            //            bw.Write(kv.Value.FileOffset);
            //        }
            //        // save words
            //        byte[] b = ms.ToArray();
            //        words.Write(b, 0, b.Length);
            //        words.Flush();
            //        words.Close();
            //    }
            //    // rename files
            //    _bitmapFile.Flush();
            //    _bitmapFile.Close();
            //    File.Delete(_Path + _FileName + ".bitmap");
            //    File.Move(_Path + _FileName + ".bitmap$", _Path + _FileName + ".bitmap");
            //    // reload everything
            //    _bitmapFile = new FileStream(_Path + _FileName + ".bitmap", FileMode.OpenOrCreate, FileAccess.ReadWrite, FileShare.ReadWrite);
            //    _lastBitmapOffset = _bitmapFile.Seek(0L, SeekOrigin.End);
            //    _internalOP = false;
            //}
        //}

        #endregion


        #region [  P R I V A T E  ]

        private WAHBitArray internalGetBitmap(int recno, bool usecache)
        {
            WAHBitArray ba = null;

            if (_cache.TryGetValue(recno, out ba))
            {
                return ba;
            }
            else
            {
                byte[] b = new byte[8];
                long off = ((long)recno) * 8;
                _recordFileRead.Seek(off, SeekOrigin.Begin);
                _recordFileRead.Read(b, 0, 8);
                long offset = Helper.ToInt64(b, 0);
                ba = LoadBitmap(offset);
                if (usecache)
                    _cache.Add(recno, ba);

                return ba;
            }
        }

        private void SaveBitmap(int recno, WAHBitArray bmp)
        {
            long offset = SaveBitmapToFile(bmp);
            long pointer = ((long)recno) * 8;
            _recordFileWrite.Seek(pointer, SeekOrigin.Begin);
            byte[] b = new byte[8];
            b = Helper.GetBytes(offset, false);
            _recordFileWrite.Write(b, 0, 8);
            //_recordFileWrite.Flush();
        }

        //-----------------------------------------------------------------
        // BITMAP FILE FORMAT
        //    0  'B','M'
        //    2  uint count = 4 bytes
        //    6  Bitmap type    0 = int record list      1 = uint bitmap
        //    7  '0'
        //    8  uint data
        //-----------------------------------------------------------------
        private long SaveBitmapToFile(WAHBitArray bmp)
        {
            long off = _lastBitmapOffset;

            uint[] bits = bmp.GetCompressed();

            byte[] b = new byte[bits.Length * 4 + 8];
            // write header data
            b[0] = ((byte)'B');
            b[1] = ((byte)'M');
            Buffer.BlockCopy(Helper.GetBytes(bits.Length, false), 0, b, 2, 4);
            b[6] = (byte)(bmp.UsingIndexes == true ? 0 : 1);
            b[7] = (byte)(0);

            for (int i = 0; i < bits.Length; i++)
            {
                byte[] u = Helper.GetBytes((int)bits[i], false);
                Buffer.BlockCopy(u, 0, b, i * 4 + 8, 4);
            }
            _bitmapFileWrite.Write(b, 0, b.Length);
            _lastBitmapOffset += b.Length;
            //_bitmapFile.Flush();
            return off;
        }

        private WAHBitArray LoadBitmap(long offset)
        {
            WAHBitArray bc = new WAHBitArray();
            if (offset == -1)
                return bc;

            List<uint> ar = new List<uint>();
            WAHBitArray.TYPE type = WAHBitArray.TYPE.Compressed_WAH;
            FileStream bmp = _bitmapFileRead;
            {
                bmp.Seek(offset, SeekOrigin.Begin);

                byte[] b = new byte[8];

                bmp.Read(b, 0, 8);
                if (b[0] == (byte)'B' && b[1] == (byte)'M' && b[7] == 0)
                {
                    type = (b[6] == 0 ? WAHBitArray.TYPE.Indexes : WAHBitArray.TYPE.Compressed_WAH);
                    int c = Helper.ToInt32(b, 2);
                    for (int i = 0; i < c; i++)
                    {
                        bmp.Read(b, 0, 4);
                        ar.Add((uint)Helper.ToInt32(b, 0));
                    }
                }
            }
            bc = new WAHBitArray(type, ar.ToArray());

            return bc;
        }
        #endregion
    }
}
