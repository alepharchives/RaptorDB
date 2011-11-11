using System;
using System.Collections.Generic;
using System.Text;
using System.IO;

namespace RaptorDB
{
    internal class LogFile<T> where T : IGetBytes<T> , IEquatable<T>, IEqualityComparer<T>
    {
        public LogFile()
        {
            _T = Activator.CreateInstance<T>();
        }

        public LogFile(string filename, int number, int maxkeylen, string logNumberFormat)
        {
            _T = Activator.CreateInstance<T>();
            // create a log file 
            FileName = filename + number.ToString(logNumberFormat);
            Number = number;
            _maxKeyLen = maxkeylen;

            _file = new FileStream(FileName, FileMode.CreateNew, FileAccess.ReadWrite, FileShare.Read);
            WriteLogHeader();
        }
        private T _T = default(T);

        public int Number;
        public string FileName;
        public bool Readonly;
        public int CurrentCount = 0;
        

        byte[] _header = new byte[] { 
            (byte)'M', (byte)'G', (byte)'L', 
            0 // [maxkeylen] 
        };

        internal SafeDictionary<T, int> _memCache = new SafeDictionary<T, int>(Global.MaxItemsBeforeIndexing, default(T));
        internal SafeDictionary<T, List<int>> _duplicates = new SafeDictionary<T, List<int>>(Global.MaxItemsBeforeIndexing , default(T));
        private Stream _file;
        private int _maxKeyLen;
        ILog log = LogManager.GetLogger(typeof(LogFile<T>));


        private void WriteLogHeader()
        {
            // write log file header;
            _header[3] = (byte)_maxKeyLen;
            _file.Write(_header, 0, _header.Length);
            _file.Flush();
        }

        public void Shutdown()
        {
            if (_file != null)
            {
                _file.Close();
                _file = null;
                _memCache = null;
            }
        }

        public void ReadLogFile(string filename)
        {
            if (File.Exists(filename))
            {
                _file = new FileStream(filename, FileMode.Open, FileAccess.Read);
                // read log header and check ok
                if (ReadLogHeader())
                {
                    // read data and put in cache
                    ReadData();
                }
                _file.Close();
                _file = null;
            }
        }

        public void DeleteLog()
        {
            Shutdown();
            log.Debug("Deleting log file : " + this.Number);
            File.Delete(FileName);
        }

        private void ReadData()
        {
            int len = 4 + 1 + _maxKeyLen;
            byte[] rec = new byte[len];
            while (_file.Read(rec, 0, len) == len)
            {
                CurrentCount++;
                int l = Helper.ToInt32(rec, 0);
                byte of = rec[4];
                T k = _T.GetObject(rec, 5, of);
                _memCache.Add(k, l);
            }
        }

        private bool ReadLogHeader()
        {
            bool ok = true;
            _file.Seek(0L, SeekOrigin.Begin);
            byte[] hdr = new byte[_header.Length];
            _file.Read(hdr, 0, _header.Length);
            for (int i = 0; i < _header.Length - 1; i++)
            {
                if (hdr[i] != _header[i])
                    ok = false;
            }
            if (ok)
                _maxKeyLen = (int)hdr[_header.Length - 1];
            return ok;
        }

        private void SaveToLogFile(T key, int offset)
        {
            if (Readonly == false)
            {
                if (_file != null)
                {
                    byte[] b = CreateRecord(key, offset);
                    _file.Write(b, 0, b.Length);
                    _file.Flush();
                }
            }
        }

        private byte[] CreateRecord(T key, int offset)
        {
            // create record
            byte[] rec = new byte[4 + 1 + _maxKeyLen];
            byte[] off = Helper.GetBytes(offset, false);
            Buffer.BlockCopy(off, 0, rec, 0, off.Length);
            byte[] str = key.GetBytes();
            rec[4] = (byte)str.Length;
            int len = (str.Length < _maxKeyLen ? str.Length : _maxKeyLen);
            Buffer.BlockCopy(str, 0, rec, off.Length + 1, len);
            return rec;
        }

        public int Get(T k)
        {
            int l = -1;
            bool b = _memCache.TryGetValue(k, out l);
            if (b)
                return l;
            else
                return -1;
        }

        public void Set(T k, int val)
        {
            if (Readonly == false)
            {
                CurrentCount++;
                int v;
                if (_memCache.TryGetValue(k, out v))
                {
                    List<int> dups;
                    if (_duplicates.TryGetValue(k, out dups))
                        dups.Add(v);
                    else
                    {
                        dups = new List<int>();
                        dups.Add(v);
                        _duplicates.Add(k, dups);
                    }
                    _memCache[k] = val;
                }
                else
                    _memCache.Add(k, val);
                // write log file
                SaveToLogFile(k, val);
            }
        }

        public void Close()
        {
            if (_file != null)
            {
                _file.Flush();
                _file.Close();
                _file = null;
            }
        }

        public List<int> GetDuplicates(T key)
        {
            List<int> dups;
            if (_duplicates.TryGetValue(key, out dups))
            {
                return dups;
            }
            return new List<int>();
        }
    }
}
