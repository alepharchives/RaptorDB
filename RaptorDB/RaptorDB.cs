using System;
using System.Collections.Generic;
using System.Text;
using System.IO;
using System.Threading;
using System.Collections;

namespace RaptorDB
{
    public enum INDEXTYPE
    {
        BTREE = 0,
        HASH = 1
    }

    public class RaptorDB
    {
        private RaptorDB(string Filename, byte MaxKeysize, bool AllowDuplicateKeys, INDEXTYPE idxtype)
        {
            Initialize(Filename, MaxKeysize, AllowDuplicateKeys, idxtype);
        }

        private List<LogFile> _logs = new List<LogFile>();
        private string _Path = "";
        private string _FileName = "";
        private LogFile _currentLOG;
        private byte _MaxKeySize;
        private StorageFile _archive;
        private IIndex _index;
        private Thread _IndexerThread;
        private Thread _ThrottleThread;
        private bool _shutdown = false;
        private string _logExtension = ".mglog";
        private string _datExtension = ".mgdat";
        private string _idxExtension = ".mgidx";
        //private string _chkExtension = ".mgchk";
        private string _logString = "000000";
        private INDEXTYPE _idxType = INDEXTYPE.BTREE;
        private long _Count = -1;
        private bool _InMemoryIndex = false;
        private bool _PauseIndex = false;
        private bool _isInternalOPRunning = false;
        private bool _throttleInput = false;

        public int IndexingTimerSeconds = 1;

        public bool FreeCacheOnCommit
        {
            get { return Global.FreeMemoryOnCommit; }
            set { Global.FreeMemoryOnCommit = value; }
        }

        public bool InMemoryIndex
        {
            get { return _InMemoryIndex; }
            set { _InMemoryIndex = value; _index.InMemory = value; }
        }

        public static RaptorDB Open(string Filename, byte MaxKeysize, bool AllowDuplicateKeys, INDEXTYPE idxtype)
        {
            return new RaptorDB(Filename, MaxKeysize, AllowDuplicateKeys, idxtype);
        }

        //public void CompactFiles()
        //{
        //    if (_isInternalOPRunning)
        //        while (_isInternalOPRunning) Thread.Sleep(10);
        //
        //    _isInternalOPRunning = true;
        //    // FIX : add compact logic here
        //
        //
        //    _isInternalOPRunning = false;
        //}

        public void SaveIndex()
        {
            SaveIndex(false);
        }

        public void SaveIndex(bool flushMemoryToDisk)
        {
            if (_isInternalOPRunning)
                while (_isInternalOPRunning) Thread.Sleep(10);

            _isInternalOPRunning = true;
            // wait for indexing to stop
            while (_indexing) Thread.Sleep(10);

            if (flushMemoryToDisk)
            {
                // flush current memory log
                NewLog();
                // flush everything to indexes
                DoIndexing(true);
            }
            _index.SaveIndex();
            // delete log files on disk
            List<string> pp = _deleteList;
            _deleteList = new List<string>();
            foreach (string s in pp)
                File.Delete(s);
            _isInternalOPRunning = false;
        }

        public IEnumerable<int> GetDuplicates(byte[] key)
        {
            if (_isInternalOPRunning)
                while (_isInternalOPRunning) Thread.Sleep(10);

            List<int> dups = new List<int>();

            // get duplicates from index
            dups.AddRange(_index.GetDuplicates(key));

            // get duplicates from memory  
            dups.AddRange(_currentLOG.GetDuplicates(key));
            foreach (var l in _logs)
                dups.AddRange(l.GetDuplicates(key));
            return dups;
        }

        public byte[] FetchDuplicate(int offset)
        {
            if (_isInternalOPRunning)
                while (_isInternalOPRunning) Thread.Sleep(10);

            return _archive.ReadData(offset);
        }

        public IEnumerable<KeyValuePair<byte[], byte[]>> EnumerateStorageFile()
        {
            if (_isInternalOPRunning)
                while (_isInternalOPRunning) Thread.Sleep(10);

            return _archive.Traverse();
        }

        //public void RemoveKey(byte[] key)
        //{
        //    if (_isInternalOPRunning)
        //        while (_isInternalOPRunning) Thread.Sleep(10);
        //    // FIX : add remove logic here
        //}

        // TODO : add enumerate keys

        //public IEnumerator Enumerate(byte[] fromkey, int start, int count)
        //{
        //    // TODO : generate a list from the start key using forward only pages
        //    List<long> l = _index.Enumerate(fromkey, start, count);

        //    return null;
        //}

        public long Count()
        {
            if (_isInternalOPRunning)
                while (_isInternalOPRunning) Thread.Sleep(10);

            if (_Count == -1)
            {
                long c = 0;
                foreach (var i in _logs)
                    c += i.CurrentCount;
                c += _currentLOG.CurrentCount;
                _Count = _index.Count() + c;
            }

            return _Count;
        }

        public bool Get(Guid guid, out byte[] val)
        {
            return Get(guid.ToByteArray(), out val);
        }

        public bool Get(string key, out byte[] val)
        {
            return Get(Helper.GetBytes(key), out val);
        }

        public bool Get(string key, out string val)
        {
            byte[] b;
            val = null;
            bool ok = Get(Helper.GetBytes(key), out b);
            if (ok)
            {
                val = Helper.GetString(b);
            }
            return ok;
        }

        public bool Get(byte[] key, out byte[] val)
        {
            if (_isInternalOPRunning)
                while (_isInternalOPRunning) Thread.Sleep(10);

            int off;
            val = null;
            byte[] k = key;
            _PauseIndex = true;
            // check in current log
            off = _currentLOG.Get(k);
            if (off > -1)
            {
                // return data here
                val = _archive.ReadData(off);
                _PauseIndex = false;
                return true;
            }
            // check in older log files
            foreach (LogFile l in _logs)
            {
                off = l.Get(k);
                if (off > -1)
                {
                    // return data here
                    val = _archive.ReadData(off);
                    _PauseIndex = false;
                    return true;
                }
            }

            // search index here
            if (_index.Get(k, out off))
            {
                val = _archive.ReadData(off);
                _PauseIndex = false;
                return true;
            }
            _PauseIndex = false;
            return false;
        }

        public bool Set(Guid guid, byte[] data)
        {
            return Set(guid.ToByteArray(), data);
        }

        public bool Set(string key, string val)
        {
            return Set(Helper.GetBytes(key), Helper.GetBytes(val));
        }

        public bool Set(string key, byte[] data)
        {
            return Set(Helper.GetBytes(key), data);
        }

        private object _lock = new object();
        public bool Set(byte[] key, byte[] data)
        {
            if (_isInternalOPRunning)
                while (_isInternalOPRunning) Thread.Sleep(10);

            int recno = -1;
            if (key.Length > _MaxKeySize)
            {
                throw new Exception("key greater than max key size of " + _MaxKeySize);
            }
            byte[] k = key;
            if (_throttleInput)
                Thread.Sleep(30);
            lock (_lock)
            {
                // save to storage
                recno = _archive.WriteData(k, data);
                // save to logfile
                _currentLOG.Set(k, recno);
                if (_currentLOG.CurrentCount > Global.MaxItemsBeforeIndexing)
                    NewLog();

                _Count++;
            }
            return true;
        }

        public void Shutdown()
        {
            _shutdown = true;
            while (_indexing)
                Thread.Sleep(50);
            if (_index != null)
                _index.Shutdown();
            if (_archive != null)
                _archive.Shutdown();
            if (_currentLOG != null)
                _currentLOG.Shutdown();
            _index = null;
            _archive = null;
            _currentLOG = null;
        }

        #region [            P R I V A T E     M E T H O D S              ]

        private void NewLog()
        {
            // new log file
            _logs.Add(_currentLOG);
            LogFile newlog = new LogFile(_Path + "\\" + _FileName + _logExtension, _currentLOG.Number + 1, _MaxKeySize, _logString);
            _currentLOG = newlog;
        }

        private void Initialize(string filename, byte maxkeysize, bool AllowDuplicateKeys, INDEXTYPE idxtype)
        {
            _idxType = idxtype;
            _MaxKeySize = maxkeysize;

            _Path = Path.GetDirectoryName(filename);
            Directory.CreateDirectory(_Path);

            _FileName = Path.GetFileNameWithoutExtension(filename);
            string db = _Path + "\\" + _FileName + _datExtension;
            string idx = _Path + "\\" + _FileName + _idxExtension;

            if (_idxType == INDEXTYPE.BTREE)
                // setup database or load database
                _index = new BTree(idx, _MaxKeySize, Global.DEFAULTNODESIZE, AllowDuplicateKeys, Global.BUCKETCOUNT);
            else
                // hash index
                _index = new Hash(idx, _MaxKeySize, Global.DEFAULTNODESIZE, AllowDuplicateKeys, Global.BUCKETCOUNT);

            _archive = new StorageFile(db, _MaxKeySize);

            _archive.SkipDateTime = true;

            // load old log files
            LoadLogFiles();
            Count();

            AppDomain.CurrentDomain.ProcessExit += new EventHandler(CurrentDomain_ProcessExit);
            // create indexing thread
            _IndexerThread = new Thread(new ThreadStart(IndexThreadRunner));
            _IndexerThread.IsBackground = true;
            _IndexerThread.Start();

            //_ThrottleThread = new Thread(new ThreadStart(Throttle));
            //_ThrottleThread.IsBackground = true;
            //_ThrottleThread.Start();
        }

        private void CurrentDomain_ProcessExit(object sender, EventArgs e)
        {
            Shutdown();
        }

        //private int _throttlecount = 0;
        //private void Throttle()
        //{
        //    while (_shutdown == false)
        //    {
        //        _throttlecount++;
        //        if (_throttlecount > 20)
        //        {
        //            _throttlecount = 0;
        //            if (Helper.GetFreeMemory() < Global.ThrottleWhenFreemMemoryLessThan)
        //                _throttleInput = true;
        //            else
        //                _throttleInput = false;
        //        }
        //    }
        //}

        private int _timercounter = 0;
        private void IndexThreadRunner()
        {
            while (_shutdown == false)
            {
                _timercounter++;
                if (_timercounter > IndexingTimerSeconds)
                {
                    DoIndexing(false);
                    _timercounter = 0;
                }

                Thread.Sleep(1000);
            }
        }

        private bool _indexing = false;
        List<string> _deleteList = new List<string>();

        private void DoIndexing(bool flushMode)
        {
            while (_logs.Count > 0)
            {
                if (_shutdown)
                {
                    _indexing = false;
                    return;
                }
                if (flushMode == false)
                    if (_isInternalOPRunning)
                        while (_isInternalOPRunning && _shutdown == false) Thread.Sleep(10);

                if (_logs.Count == 0)
                    return;
                _indexing = true;
                LogFile l = _logs[0];

                if (_logs.Count > Global.ThrottleInputWhenLogCount)
                    _throttleInput = true;
                else
                    _throttleInput = false;

                if (l != null)
                {
                    // save duplicates
                    if (l._duplicates != null)
                    {
                        #region index memory duplicates
                        foreach (KeyValuePair<byte[], List<int>> kv in l._duplicates)
                        {
                            if (kv.Value != null)
                            {
                                foreach (int off in kv.Value)
                                {
                                    _index.Set(kv.Key, off);
                                    
                                    if (flushMode == false)
                                    {
                                        if (_shutdown)
                                        {
                                            _index.Commit();
                                            _indexing = false;
                                            return;
                                        }
                                        while (_PauseIndex)
                                            Thread.Sleep(10);
                                    }
                                }
                            }
                        }
                        #endregion
                    }
                    foreach (KeyValuePair<byte[], int> kv in l._memCache)
                    {
                        #region index data in cache
                        _index.Set(kv.Key, kv.Value);
                        
                        if (flushMode == false)
                        {
                            if (_shutdown)
                            {
                                _index.Commit();
                                _indexing = false;
                                return;
                            }
                            while (_PauseIndex)
                                Thread.Sleep(10);
                        }
                        #endregion
                    }
                    _index.Commit();
                    l.Close();
                    _logs.Remove(l);
                    if (_index.InMemory == false)
                        l.DeleteLog();
                    else
                        _deleteList.Add(l.FileName);
                    l = null;
                }
                //GC.Collect(2);
                _indexing = false;
                if (flushMode == false)
                {
                    if (_logs.Count == 1)
                        return;
                }
            }
        }

        private void LoadLogFiles()
        {
            string[] fnames = Directory.GetFiles(_Path, _FileName + _logExtension + "*", SearchOption.TopDirectoryOnly);
            Array.Sort(fnames);
            if (fnames.Length > 0)
            {
                int i = 0;
                // rename log file to start from 0
                foreach (string f in fnames)
                {
                    File.Move(f, _Path + "\\" + _FileName + _logExtension + i.ToString(_logString));
                    i++;
                }
            }

            fnames = Directory.GetFiles(_Path, _FileName + _logExtension + "*", SearchOption.TopDirectoryOnly);
            Array.Sort(fnames);
            int lognum = 0;

            foreach (string fn in fnames)
            {
                if (File.Exists(fn))
                {
                    // Parse extension number
                    LogFile l = new LogFile();
                    l.FileName = fn;
                    l.Readonly = false;
                    l.Number = lognum++;

                    // load log data data
                    l.ReadLogFile(fn);

                    l.Readonly = true;
                    _logs.Add(l);
                }
            }
            _currentLOG = new LogFile(_Path + "\\" + _FileName + _logExtension, lognum, _MaxKeySize, _logString);
        }
        #endregion
    }
}
