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

    public class RaptorDB<T> : IDisposable where T : IRDBDataType<T>
    {
        public RaptorDB(string Filename, byte MaxKeysize, bool AllowDuplicateKeys, INDEXTYPE idxtype, bool disablethread)
        {
            Initialize(Filename, MaxKeysize, AllowDuplicateKeys, idxtype, disablethread);
        }

        private ILog log = LogManager.GetLogger(typeof(RaptorDB<T>));

        private List<LogFile<T>> _logs = new List<LogFile<T>>();
        private string _Path = "";
        private string _FileName = "";
        private LogFile<T> _currentLOG;
        private byte _MaxKeySize;
        private StorageFile _archive;
        private IIndex<T> _index;
        private Thread _IndexerThread;
        private bool _shutdown = false;
        private string _logExtension = ".mglog";
        private string _datExtension = ".mgdat";
        private string _idxExtension = ".mgidx";
        //private string _chkExtension = ".mgchk";
        private string _logString = "000000";
        private INDEXTYPE _idxType = INDEXTYPE.BTREE;
        private long _Count = -1;
        private bool _InMemoryIndex = true;
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

        public static RaptorDB<T> Open(string Filename, byte MaxKeysize, bool AllowDuplicateKeys, INDEXTYPE idxtype)
        {
            return new RaptorDB<T>(Filename, MaxKeysize, AllowDuplicateKeys, idxtype, false);
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
            SaveIndex(true);
        }

        private void SaveIndex(bool flushMemoryToDisk)
        {
            PauseForInternalOP();
            log.Debug("flushing : " + flushMemoryToDisk);
            log.Debug("paused : " + _PauseIndex);
            log.Debug("indexing : " + _indexing);
            _isInternalOPRunning = true;
            // wait for indexing thread to stop
            while (_indexing) Thread.Sleep(10);
               
            if (flushMemoryToDisk)
            {
                // flush current memory log
                NewLog();
                // flush everything to indexes
                DoIndexing(true);
            }
            log.Debug("saving to file");
            internalSaveIndex();
            _isInternalOPRunning = false;
        }

        private void internalSaveIndex()
        {
            _index.SaveIndex();
            log.Debug("index saved");
            // delete log files on disk
            List<string> pp = _deleteList;
            _deleteList = new List<string>();
            foreach (string s in pp)
                File.Delete(s);
        }

        public IEnumerable<int> GetDuplicates(T key)
        {
            PauseForInternalOP();

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
            PauseForInternalOP();

            return _archive.ReadData(offset);
        }

        public IEnumerable<KeyValuePair<byte[], byte[]>> EnumerateStorageFile()
        {
            PauseForInternalOP();

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
            if (_Count == -1)
            {
                //long c = 0;
                //foreach (var i in _logs)
                //    c += i.CurrentCount;
                //c += _currentLOG.CurrentCount;
                _Count = _archive.Count();// +c;
            }

            return _Count;
        }

        public bool Get(T key, out byte[] val)
        {
            PauseForInternalOP();

            int off;
            val = null;
            T k = key;
            _PauseIndex = true;
            while (_indexing) Thread.Sleep(1);
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
            foreach (LogFile<T> l in _logs)
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

        private object _lock = new object();
        public bool Set(T key, byte[] data)
        {
            PauseForInternalOP();

            int recno = -1;
            //if (_throttleInput)
            //    Thread.Sleep(1);
            lock (_lock)
            {
                // save to storage
                byte[] kkey = key.GetBytes();
                recno = _archive.WriteData(kkey, data);
                // save to logfile
                _currentLOG.Set(key, recno);
                if (_currentLOG.CurrentCount > Global.MaxItemsBeforeIndexing)
                    NewLog();

                _Count++;
            }
            return true;
        }

        public void Shutdown()
        {
            if (_index != null)
            {
                log.Debug("Shutting down");
            }
            Console.WriteLine("Shutting down...");
            //_shutdown = true;

            SaveIndex(true);
            _shutdown = true;
            
            if (_index != null)
                _index.Shutdown();
            if (_archive != null)
                _archive.Shutdown();
            if (_currentLOG != null)
                _currentLOG.Shutdown();
            if (_logs != null)
            {
                foreach (var l in _logs)
                {
                    if (l != null)
                        l.Close();
                }
            }
            _logs = null;
            _index = null;
            _archive = null;
            _currentLOG = null;
            log.Debug("Shutting down log");
            _IndexerThread = null;
            LogManager.Shutdown();
        }

        #region [            P R I V A T E     M E T H O D S              ]

        private void PauseForInternalOP()
        {
            if (_isInternalOPRunning)
                while (_isInternalOPRunning) Thread.Sleep(10);
        }

        private void NewLog()
        {
            // new log file
            _logs.Add(_currentLOG);
            LogFile<T> newlog = new LogFile<T>(_Path + "\\" + _FileName + _logExtension, _currentLOG.Number + 1, _MaxKeySize, _logString);
            _currentLOG = newlog;
        }

        private void Initialize(string filename, byte maxkeysize, bool AllowDuplicateKeys, INDEXTYPE idxtype, bool disablethread)
        {
            _idxType = idxtype;
            _MaxKeySize = maxkeysize;

            _Path = Path.GetDirectoryName(filename);
            Directory.CreateDirectory(_Path);

            _FileName = Path.GetFileNameWithoutExtension(filename);
            string db = _Path + "\\" + _FileName + _datExtension;
            string idx = _Path + "\\" + _FileName + _idxExtension;

            LogManager.Configure(_Path + "\\" + _FileName + ".txt", 500, false);

            if (_idxType == INDEXTYPE.BTREE)
                // setup database or load database
                _index = new BTree<T>(idx, _MaxKeySize, Global.DEFAULTNODESIZE, AllowDuplicateKeys, Global.BUCKETCOUNT);
            else
                // hash index
                _index = new Hash<T>(idx, _MaxKeySize, Global.DEFAULTNODESIZE, AllowDuplicateKeys, Global.BUCKETCOUNT);

            _archive = new StorageFile(db, _MaxKeySize);

            _archive.SkipDateTime = true;

            // load old log files
            LoadLogFiles();
            Count();
            log.Debug("Current Count = " + _Count.ToString("#,#"));

            log.Debug("Starting indexer thread");
            // create indexing thread
            if (disablethread == false)
                StartIndexerThread();

            //_ThrottleThread = new Thread(new ThreadStart(Throttle));
            //_ThrottleThread.IsBackground = true;
            //_ThrottleThread.Start();
        }

        internal void StartIndexerThread()
        {
            _IndexerThread = new Thread(new ThreadStart(IndexThreadRunner));
            _IndexerThread.IsBackground = true;
            _IndexerThread.Start();
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
                if (_timercounter > IndexingTimerSeconds &&
                    _isInternalOPRunning == false &&
                    _shutdown == false)
                {
                    try
                    {
                        DoIndexing(false);
                    }
                    catch (Exception ex)
                    {
                        log.Error("" + ex);
                    }
                    _timercounter = 0;
                }
                Thread.Sleep(1000);
            }
            _indexing = false;
            log.Debug("Indexer Thread done.");
            _IndexerThread = null;
        }

        private bool _indexing = false;
        List<string> _deleteList = new List<string>();
        private int _currCount = 0;
        private int Million = 1000000;
        private object _doindxlock = new object();

        private void DoIndexing(bool flushMode)
        {
            lock (_doindxlock)
            {
                _indexing = false;
                while (_logs.Count > 0)
                {
                    log.Debug("DoIndexing");
                    _indexing = false;
                    if (_shutdown)
                        return;

                    log.Debug("log count = " + _logs.Count);
                    if (_logs.Count == 0)
                        return;

                    LogFile<T> l = _logs[0];

                    if (l != null)
                    {
                        log.Debug("starting indexing on log # : " + l.Number);
                        _indexing = true;
                        // save duplicates
                        if (l._duplicates != null)
                        {
                            #region index memory duplicates
                            foreach (KeyValuePair<T, List<int>> kv in l._duplicates)
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
                                                log.Debug("shutting down in middle of duplicate processing");
                                                _indexing = false;
                                                //_index.Commit();
                                                return;
                                            }
                                            while (_PauseIndex && _shutdown == false)
                                            {
                                                Thread.Sleep(10);
                                            }
                                        }
                                    }
                                }
                            }
                            #endregion
                        }
                        foreach (KeyValuePair<T, int> kv in l._memCache)
                        {
                            #region index data in cache
                            _index.Set(kv.Key, kv.Value);

                            if (flushMode == false)
                            {
                                if (_shutdown)
                                {
                                    log.Debug("shutting down in middle of data processing");
                                    _indexing = false;
                                    //_index.Commit();
                                    return;
                                }
                                while (_PauseIndex && _shutdown == false)
                                {
                                    Thread.Sleep(10);
                                }
                            }
                            #endregion
                        }
                        log.Debug("commit index");
                        _index.Commit();
                        l.Close();
                        _logs.Remove(l);
                        _currCount++;
                        if (flushMode == false)
                        {
                            // start flushing after this mil every 300k
                            if (_currCount > 15 && _Count > ((long)Global.FlushIndexerAfterThisManyMillion) * Million)
                            {
                                log.Debug("flushing index to disk");
                                internalSaveIndex();
                                _currCount = 0;
                            }
                        }
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
        }

        private void LoadLogFiles()
        {
            log.Debug("Loading log files");
            string[] fnames = Directory.GetFiles(_Path, _FileName + _logExtension + "*", SearchOption.TopDirectoryOnly);
            Array.Sort(fnames);
            log.Debug("log count = " + fnames.Length);
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
                    LogFile<T> l = new LogFile<T>();
                    l.FileName = fn;
                    l.Readonly = false;
                    l.Number = lognum++;

                    // load log data data
                    l.ReadLogFile(fn);

                    l.Readonly = true;
                    _logs.Add(l);
                }
            }
            _currentLOG = new LogFile<T>(_Path + "\\" + _FileName + _logExtension, lognum, _MaxKeySize, _logString);
        }
        #endregion

        public void Dispose()
        {
            Shutdown();
        }
    }
}
