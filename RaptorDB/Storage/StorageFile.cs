using System;
using System.Diagnostics;
using System.Collections;
using System.Runtime.InteropServices;
using System.IO;
using System.Text;
using System.Collections.Generic;

namespace RaptorDB
{
    internal class StorageFile<T>
    {
        System.IO.FileStream _writefile;
        System.IO.FileStream _recordfile;
        private int _maxKeyLen;
        string _filename = "";
        string _recfilename = "";
        int _lastRecordNum = 0;
        long _lastWriteOffset = 0;
        IGetBytes<T> _T = null;

        public static byte[] _fileheader = { (byte)'M', (byte)'G', (byte)'D', (byte)'B',
                                              0, // -- [flags] = [shutdownOK:1],
                                              0  // -- [maxkeylen] 
                                           };

        public static byte[] _rowheader = { (byte)'M', (byte)'G', (byte)'R' ,
                                           0,               // 3     [keylen]
                                           0,0,0,0,0,0,0,0, // 4-11  [datetime] 8 bytes = insert time
                                           0,0,0,0,         // 12-15 [data length] 4 bytes
                                           0,               // 16 -- [flags] = 1 : isDeletd:1
                                                            //                 2 : isCompressed:1
                                                            //                 
                                                            //                 
                                           0                // 17 -- [crc] = header crc check
                                       };
        private enum HDR_POS
        {
            KeyLen = 3,
            DateTime = 4,
            DataLength = 12,
            Flags = 16,
            CRC = 17
        }

        public bool SkipDateTime = false;
        private bool _flushNeeded = false;

        public StorageFile(string filename, int maxkeylen)
        {
            _T = RDBDataType<T>.ByteHandler();
            _filename = filename;
            if (File.Exists(filename) == false)
                _writefile = new FileStream(filename, FileMode.CreateNew, FileAccess.ReadWrite, FileShare.ReadWrite);
            else
                _writefile = new FileStream(filename, FileMode.Open, FileAccess.ReadWrite, FileShare.ReadWrite);

            // load rec pointers
            _recfilename = filename.Substring(0, filename.LastIndexOf('.')) + ".mgrec";
            if (File.Exists(_recfilename) == false)
                _recordfile = new FileStream(_recfilename, FileMode.CreateNew, FileAccess.ReadWrite, FileShare.ReadWrite);
            else
                _recordfile = new FileStream(_recfilename, FileMode.Open, FileAccess.ReadWrite, FileShare.ReadWrite);

            _maxKeyLen = maxkeylen;
            if (_writefile.Length == 0)
            {
                // new file
                byte b = (byte)maxkeylen;
                _fileheader[5] = b;
                _writefile.Write(_fileheader, 0, _fileheader.Length);
                _writefile.Flush();
            }

            bw = new BinaryWriter(ms, Encoding.UTF8);

            _lastRecordNum = (int)(_recordfile.Length / 8);
            _recordfile.Seek(0L, SeekOrigin.End);
            _lastWriteOffset = _writefile.Seek(0L, SeekOrigin.End);
        }

        public int Count()
        {
            return (int)(_recordfile.Length >> 3);
        }

        public IEnumerable<KeyValuePair<T, byte[]>> Traverse()
        {
            long offset = 0;
            offset = _fileheader.Length;

            while (offset < _writefile.Length)
            {
                long pointer = offset;
                byte[] key;
                bool deleted = false;
                offset = NextOffset(offset, out key, out deleted);
                KeyValuePair<T, byte[]> kv = new KeyValuePair<T, byte[]>(_T.GetObject(key, 0, key.Length), internalReadData(pointer));
                if (deleted == false)
                    yield return kv;
            }
        }

        private long NextOffset(long curroffset, out byte[] key, out bool isdeleted)
        {
            isdeleted = false;
            if (_readdata == null)
                _readdata = new FileStream(_filename, FileMode.Open, FileAccess.Read, FileShare.ReadWrite);
            {
                long next = _readdata.Length;
                // seek offset in file
                byte[] hdr = new byte[_rowheader.Length];
                _readdata.Seek(curroffset, System.IO.SeekOrigin.Begin);
                // read header
                _readdata.Read(hdr, 0, _rowheader.Length);
                key = new byte[hdr[(int)HDR_POS.KeyLen]];
                _readdata.Read(key, 0, hdr[(int)HDR_POS.KeyLen]);
                // check header
                if (CheckHeader(hdr))
                {
                    next = curroffset + hdr.Length + Helper.ToInt32(hdr, (int)HDR_POS.DataLength) + hdr[(int)HDR_POS.KeyLen];
                    isdeleted = isDeleted(hdr);
                }

                return next;
            }
        }

        public int WriteData(T key, byte[] data, bool deleted)
        {
            byte[] k = _T.GetBytes(key);
            int kl = k.Length;

            // seek end of file
            long offset = _lastWriteOffset;
            byte[] hdr = CreateRowHeader(kl, (data==null?0:data.Length));
            if (deleted)
                hdr[(int)HDR_POS.Flags] = (byte)1;
            // write header info
            _writefile.Write(hdr, 0, hdr.Length);
            // write key
            _writefile.Write(k, 0, kl);
            if (data != null)
            {
                // write data block
                _writefile.Write(data, 0, data.Length);
                if (Global.FlushStorageFileImmetiatley)
                    _writefile.Flush();
                _lastWriteOffset += data.Length;
            }
            // update pointer
            _lastWriteOffset += hdr.Length;
            _lastWriteOffset += kl;
            // return starting offset -> recno
            int recno = _lastRecordNum++;
            _recordfile.Write(Helper.GetBytes(offset, false), 0, 8);
            if (Global.FlushStorageFileImmetiatley)
                _recordfile.Flush();
            _flushNeeded = true;
            return recno;
        }

        MemoryStream ms = new MemoryStream();
        BinaryWriter bw;
        private byte[] CreateRowHeader(int keylen, int datalen)
        {
            ms.Seek(0L, SeekOrigin.Begin);
            bw.Write(_rowheader, 0, 3);
            bw.Write((byte)keylen);
            if (SkipDateTime == false)
                bw.Write(FastDateTime.Now.Ticks);
            else
                bw.Write(0L);
            bw.Write(datalen);
            bw.Write((byte)0);
            bw.Write((byte)0);
            bw.Flush();
            return ms.ToArray();
        }

        FileStream _read = null;
        public byte[] ReadData(int recnum)
        {
            if (_flushNeeded)
            {
                _writefile.Flush();
                _recordfile.Flush();
                _flushNeeded = false;
            }
            long off = recnum * 8;
            if (_read == null)
                _read = new FileStream(_recfilename, FileMode.Open, FileAccess.Read, FileShare.ReadWrite);

            byte[] b = new byte[8];

            _read.Seek(off, SeekOrigin.Begin);
            _read.Read(b, 0, 8);
            off = Helper.ToInt64(b, 0);

            return internalReadData(off);
        }

        FileStream _readdata = null;
        private byte[] internalReadData(long offset)
        {
            if (_readdata == null)
                _readdata = new FileStream(_filename, FileMode.Open, FileAccess.Read, FileShare.ReadWrite);

            // seek offset in file
            byte[] hdr = new byte[_rowheader.Length];
            _readdata.Seek(offset, System.IO.SeekOrigin.Begin);
            // read header
            _readdata.Read(hdr, 0, _rowheader.Length);
            // check header
            if (CheckHeader(hdr))
            {
                // skip key bytes
                _readdata.Seek(hdr[(int)HDR_POS.KeyLen], System.IO.SeekOrigin.Current);
                int dl = Helper.ToInt32(hdr, (int)HDR_POS.DataLength);
                byte[] data = new byte[dl];
                // read data block
                _readdata.Read(data, 0, dl);
                return data;
            }
            else
                throw new Exception("data header error");
        }

        private bool CheckHeader(byte[] hdr)
        {
            if (hdr[0] == (byte)'M' && hdr[1] == (byte)'G' && hdr[2] == (byte)'R' && hdr[(int)HDR_POS.CRC] == (byte)0)
                return true;
            return false;
        }

        public void Shutdown()
        {
            FlushClose(_readdata);
            FlushClose(_read);
            FlushClose(_recordfile);
            FlushClose(_writefile);

            _readdata = null;
            _read = null;
            _recordfile = null;
            _writefile = null;
        }

        private void FlushClose(FileStream st)
        {
            if (st != null)
            {
                st.Flush(true);
                st.Close();
            }
        }

        internal T GetKey(int recnum, out bool deleted)
        {
            deleted = false;
            long off = recnum * 8;
            if (_read == null)
                _read = new FileStream(_recfilename, FileMode.Open, FileAccess.Read, FileShare.ReadWrite);

            byte[] b = new byte[8];

            _read.Seek(off, SeekOrigin.Begin);
            _read.Read(b, 0, 8);
            off = Helper.ToInt64(b, 0);

            if (_readdata == null)
                _readdata = new FileStream(_filename, FileMode.Open, FileAccess.Read, FileShare.ReadWrite);

            // seek offset in file
            byte[] hdr = new byte[_rowheader.Length];
            _readdata.Seek(off, System.IO.SeekOrigin.Begin);
            // read header
            _readdata.Read(hdr, 0, _rowheader.Length);

            if (CheckHeader(hdr))
            {
                deleted = isDeleted(hdr);
                byte kl = hdr[3];
                byte[] kbyte = new byte[kl];

                _readdata.Read(kbyte, 0, kl);
                return _T.GetObject(kbyte, 0, kl);
            }

            return default(T);
        }

        private bool isDeleted(byte[] hdr)
        {
            return (hdr[(int)HDR_POS.Flags] & (byte)1) > 0;
        }
    }
}
