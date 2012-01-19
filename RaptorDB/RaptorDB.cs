﻿using System;
using System.Collections.Generic;
using System.Text;
using System.IO;
using System.Threading;
using System.Collections;

namespace RaptorDB
{
    // FIX : add fetchrecord, enumerate, getduplicates
    public class RaptorDBString : IDisposable
    {
        public RaptorDBString(string filename, bool caseSensitve)
        {
            _db = RaptorDB<int>.Open(filename, true);
            _caseSensitive = caseSensitve;
        }
        bool _caseSensitive = false;

        RaptorDB<int> _db;


        public void Set(string key, string val)
        {
            Set(key, Encoding.Unicode.GetBytes(val));
        }

        public void Set(string key, byte[] val)
        {
            string str = (_caseSensitive ? key : key.ToLower());
            byte[] bkey = Encoding.Unicode.GetBytes(str);
            int hc = (int)Helper.MurMur.Hash(bkey);
            MemoryStream ms = new MemoryStream();
            ms.Write(Helper.GetBytes(bkey.Length, false), 0, 4);
            ms.Write(bkey, 0, bkey.Length);
            ms.Write(val, 0, val.Length);

            _db.Set(hc, ms.ToArray());
        }

        public bool Get(string key, out string val)
        {
            val = null;
            byte[] bval;
            bool b = Get(key, out bval);
            if (b)
            {
                val = Encoding.Unicode.GetString(bval);
            }
            return b;
        }

        public bool Get(string key, out byte[] val)
        {
            string str = (_caseSensitive ? key : key.ToLower());
            val = null;
            byte[] bkey = Encoding.Unicode.GetBytes(str);
            int hc = (int)Helper.MurMur.Hash(bkey);

            if (_db.Get(hc, out val))
            {
                // unpack data
                byte[] g = null;
                if (UnpackData(val, out val, out g))
                {
                    if (Helper.CompareMemCmp(bkey, g) != 0)
                    {
                        // if data not equal check duplicates (hash conflict)
                        List<int> ints = new List<int>(_db.GetDuplicates(hc));
                        ints.Reverse();
                        foreach (int i in ints)
                        {
                            byte[] bb = _db.FetchRecordBytes(i);
                            if (UnpackData(bb, out val, out g))
                            {
                                if (Helper.CompareMemCmp(bkey, g) == 0)
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

        public long Count(bool includeDuplicates)
        {
            return _db.Count(includeDuplicates);
        }

        public void SaveIndex()
        {
            _db.SaveIndex();
        }

        public void Shutdown()
        {
            _db.Shutdown();
        }

        public void Dispose()
        {
            _db.Shutdown();
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
    }

    // FIX : add fetchrecord, enumerate, getduplicates
    public class RaptorDBGuid : IDisposable
    {
        public RaptorDBGuid(string filename)
        {
            _db = RaptorDB<int>.Open(filename, true);
        }

        RaptorDB<int> _db;

        public void Set(Guid key, string val)
        {
            Set(key, Encoding.Unicode.GetBytes(val));
        }

        public void Set(Guid key, byte[] val)
        {
            byte[] bkey = key.ToByteArray();
            int hc = (int)Helper.MurMur.Hash(bkey);
            MemoryStream ms = new MemoryStream();
            ms.Write(Helper.GetBytes(bkey.Length, false), 0, 4);
            ms.Write(bkey, 0, bkey.Length);
            ms.Write(val, 0, val.Length);

            _db.Set(hc, ms.ToArray());
        }

        public bool Get(Guid key, out string val)
        {
            val = null;
            byte[] bval;
            bool b = Get(key, out bval);
            if (b)
            {
                val = Encoding.Unicode.GetString(bval);
            }
            return b;
        }

        public bool Get(Guid key, out byte[] val)
        {
            val = null;
            byte[] bkey = key.ToByteArray();
            int hc = (int)Helper.MurMur.Hash(bkey);

            if (_db.Get(hc, out val))
            {
                // unpack data
                byte[] g = null;
                if (UnpackData(val, out val, out g))
                {
                    if (Helper.CompareMemCmp(bkey, g) != 0)
                    {
                        // if data not equal check duplicates (hash conflict)
                        List<int> ints = new List<int>(_db.GetDuplicates(hc));
                        ints.Reverse();
                        foreach (int i in ints)
                        {
                            byte[] bb = _db.FetchRecordBytes(i);
                            if (UnpackData(bb, out val, out g))
                            {
                                if (Helper.CompareMemCmp(bkey, g) == 0)
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

        public long Count(bool includeDuplicates)
        {
            return _db.Count(includeDuplicates);
        }

        public void SaveIndex()
        {
            _db.SaveIndex();
        }

        public void Shutdown()
        {
            _db.Shutdown();
        }

        public void Dispose()
        {
            _db.Shutdown();
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
    }

    public class RaptorDB<T> : IDisposable where T : IComparable<T>
    {
        public RaptorDB(string Filename, byte MaxKeySize, bool AllowDuplicateKeys)
        {
            Initialize(Filename, MaxKeySize, AllowDuplicateKeys);
        }

        public RaptorDB(string Filename, bool AllowDuplicateKeys)
        {
            Initialize(Filename, Global.DefaultStringKeySize, AllowDuplicateKeys);
        }

        private ILog log = LogManager.GetLogger(typeof(RaptorDB<T>));

        private string _Path = "";
        private string _FileName = "";
        private byte _MaxKeySize;
        private StorageFile<T> _archive;
        private MGIndex<T> _index;
        private string _datExtension = ".mgdat";
        private string _idxExtension = ".mgidx";
        //private long _Count = -1;
        private System.Timers.Timer _savetimer;


        public static RaptorDB<T> Open(string Filename, bool AllowDuplicateKeys)
        {
            return new RaptorDB<T>(Filename, AllowDuplicateKeys);
        }

        public static RaptorDB<T> Open(string Filename, byte MaxKeySize, bool AllowDuplicateKeys)
        {
            return new RaptorDB<T>(Filename, MaxKeySize, AllowDuplicateKeys);
        }

        object _savelock = new object();
        public void SaveIndex()
        {
            if (_index == null)
                return;
            lock (_savelock)
            {
                log.Debug("saving to disk");
                _index.SaveIndex();
                log.Debug("index saved");
            }
        }

        public IEnumerable<int> GetDuplicates(T key)
        {
            // get duplicates from index
            return _index.GetDuplicates(key);
        }

        public byte[] FetchRecordBytes(int record)
        {
            return _archive.ReadData(record);
        }

        public string FetchRecordString(int record)
        {
            byte[] b = _archive.ReadData(record);

            return Encoding.Unicode.GetString(b);
        }

        public IEnumerable<KeyValuePair<T, byte[]>> EnumerateStorageFile()
        {
            return _archive.Traverse();
        }

        public IEnumerable<KeyValuePair<T,int>> Enumerate(T fromkey)//, bool includeDuplicates, int start, int count)
        {
            lock (_lock)
            {
                // generate a list from the start key using forward only pages
                return _index.Enumerate(fromkey);//, includeDuplicates, start, count);
            }
        }

        public bool RemoveKey(T key)
        {
            // remove and store key in storage file
            _archive.WriteData(key, null, true);
            return _index.RemoveKey(key);
        }

        public long Count(bool includeDuplicates)
        {
            return _index.Count(includeDuplicates);
        }

        public bool Get(T key, out string val)
        {
            byte[] b = null;
            val = "";
            bool ret = Get(key, out b);
            if (ret)
                val = Encoding.Unicode.GetString(b);
            return ret;
        }

        public bool Get(T key, out byte[] val)
        {
            int off;
            val = null;
            T k = key;
            lock (_lock)
            {
                // search index
                if (_index.Get(k, out off))
                {
                    val = _archive.ReadData(off);
                    return true;
                }
                return false;
            }
        }

        public bool Set(T key, string data)
        {
            return Set(key, Encoding.Unicode.GetBytes(data));
        }

        private object _lock = new object();
        public bool Set(T key, byte[] data)
        {
            int recno = -1;
            lock (_lock)
            {
                // save to storage
                recno = _archive.WriteData(key, data, false);
                // save to index
                _index.Set(key, recno);

                //_Count++;
            }
            return true;
        }

        private object _shutdownlock = new object();
        public void Shutdown()
        {
            lock (_shutdownlock)
            {
                if (_index != null)
                    log.Debug("Shutting down");
                else
                    return;
                SaveIndex();
                SaveLastRecord();

                if (_index != null)
                    _index.Shutdown();
                if (_archive != null)
                    _archive.Shutdown();
                _index = null;
                _archive = null;
                log.Debug("Shutting down log");
                LogManager.Shutdown();
            }
        }

        public Statistics GetStatistics()
        {
            return _index.GetStatistics();
        }

        #region [            P R I V A T E     M E T H O D S              ]
        private void SaveLastRecord()
        {
            // save the last record number in the index file
            _index.SaveLastRecordNumber(_archive.Count());
        }

        private void Initialize(string filename, byte maxkeysize, bool AllowDuplicateKeys)
        {
            _MaxKeySize = RDBDataType<T>.GetByteSize(maxkeysize);

            _Path = Path.GetDirectoryName(filename);
            Directory.CreateDirectory(_Path);

            _FileName = Path.GetFileNameWithoutExtension(filename);
            string db = _Path + "\\" + _FileName + _datExtension;
            string idx = _Path + "\\" + _FileName + _idxExtension;

            LogManager.Configure(_Path + "\\" + _FileName + ".txt", 500, false);

            _index = new MGIndex<T>(_Path, _FileName + _idxExtension, _MaxKeySize, Global.PageItemCount, AllowDuplicateKeys);

            _archive = new StorageFile<T>(db, _MaxKeySize);

            _archive.SkipDateTime = true;

            log.Debug("Current Count = " + Count(false).ToString("#,0"));

            CheckIndexState();

            log.Debug("Starting save timer");
            _savetimer = new System.Timers.Timer();
            _savetimer.Elapsed += new System.Timers.ElapsedEventHandler(_savetimer_Elapsed);
            _savetimer.Interval = Global.SaveTimerSeconds * 1000;
            _savetimer.AutoReset = true;
            _savetimer.Start();

        }

        private void CheckIndexState()
        {
            log.Debug("Checking Index state...");
            int last = _index.GetLastIndexedRecordNumber();
            int count = _archive.Count();
            if (last < count)
            {
                log.Debug("Rebuilding index...");
                log.Debug("   last index count = " + last);
                log.Debug("   data items count = " + count);
                // check last index record and archive record
                //       rebuild index if needed
                for (int i = last; i < count; i++)
                {
                    bool deleted = false;
                    T key = _archive.GetKey(i, out deleted);
                    if (deleted == false)
                        _index.Set(key, i);
                    else
                        _index.RemoveKey(key);

                    if (i % 100000 == 0)
                        log.Debug("100,000 items re-indexed");
                }
                log.Debug("Rebuild index done.");
            }
        }

        void _savetimer_Elapsed(object sender, System.Timers.ElapsedEventArgs e)
        {
            SaveIndex();
        }

        #endregion

        public void Dispose()
        {
            Shutdown();
        }
    }
}
