﻿using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;

namespace RaptorDB
{
    #region [ internal classes ]
    internal struct PageInfo
    {
        public PageInfo(int pagenum, int uniquecount, int duplicatecount)
        {
            PageNumber = pagenum;
            UniqueCount = uniquecount;
        }
        public int PageNumber;
        public int UniqueCount;
    }

    internal struct KeyInfo
    {
        public KeyInfo(int recnum)
        {
            RecordNumber = recnum;
            DuplicateBitmapNumber = -1;
        }
        public KeyInfo(int recnum, int bitmaprec)
        {
            RecordNumber = recnum;
            DuplicateBitmapNumber = bitmaprec;
        }
        public int RecordNumber;
        public int DuplicateBitmapNumber;
    }

    internal struct Page<T>
    {
        public Page(bool b) // kludge so the compiler doesn't complain
        {
            DiskPageNumber = -1;
            RightPageNumber = -1;
            tree = new SafeDictionary<T, KeyInfo>(Global.PageItemCount);
            isDirty = false;
            FirstKey = default(T);
        }
        public int DiskPageNumber;
        public int RightPageNumber;
        public T FirstKey;
        public bool isDirty;
        public SafeDictionary<T, KeyInfo> tree;
    }


    public class Statistics
    {
        public int PageCount = 0;
        public double TotalSplitTime = 0;
        public double FillFactor = 0;

        public override string ToString()
        {
            string s = "Page Count = " + PageCount + ", Total Split Time = " + TotalSplitTime;// +", Fill Factor = " + FillFactor;
            return s;
        }
    }
    #endregion

    internal class MGIndex<T> where T : IComparable<T>
    {
        ILog _log = LogManager.GetLogger(typeof(MGIndex<T>));
        private SortedList<T, PageInfo> _pageList = new SortedList<T, PageInfo>();
        private SafeDictionary<int, Page<T>> _cache = new SafeDictionary<int, Page<T>>();
        private List<int> _pageListDiskPages = new List<int>();
        private IndexFile<T> _index;
        private bool _AllowDuplicates = true;
        private int _LastIndexedRecordNumber = 0;

        public MGIndex(string path, string filename, byte keysize, ushort maxcount, bool allowdups)
        {
            _AllowDuplicates = allowdups;
            _index = new IndexFile<T>(path + "\\" + filename, keysize, maxcount);

            // load page list
            _index.GetPageList(_pageListDiskPages, _pageList, out _LastIndexedRecordNumber);
            if (_pageList.Count == 0)
            {
                Page<T> page = new Page<T>(false);
                page.FirstKey = (T)RDBDataType<T>.GetEmpty();
                page.DiskPageNumber = _index.GetNewPageNumber();
                page.isDirty = true;
                _pageList.Add(page.FirstKey, new PageInfo(page.DiskPageNumber, 0, 0));
                _cache.Add(page.DiskPageNumber, page);
            }
        }

        public int Count(bool includeDuplicates)
        {
            int i = 0;
            foreach (var k in _pageList)
            {
                i += k.Value.UniqueCount;
                if (includeDuplicates)
                {
                    // FEATURE : count duplicates
                }
            }

            return i;
        }

        public int GetLastIndexedRecordNumber()
        {
            return _LastIndexedRecordNumber;
        }

        public void Set(T key, int val)
        {
            PageInfo pi;
            Page<T> page = LoadPage(key, out pi);

            KeyInfo ki;
            if (page.tree.TryGetValue(key, out ki))
            {
                // item exists
                if (_AllowDuplicates)
                    SaveDuplicate(key, ref ki);
                ki.RecordNumber = val;
                page.tree[key] = ki; // structs need resetting
            }
            else
            {
                // new item 
                ki = new KeyInfo(val);
                pi.UniqueCount++;
                page.tree.Add(key, ki);
            }

            if (page.tree.Count > Global.PageItemCount)
                SplitPage(page);

            _LastIndexedRecordNumber = val;
            page.isDirty = true;
        }

        public bool Get(T key, out int val)
        {
            val = -1;
            PageInfo pi;
            Page<T> page = LoadPage(key, out pi);
            KeyInfo ki;
            bool ret = page.tree.TryGetValue(key, out ki);
            if (ret)
                val = ki.RecordNumber;
            return ret;
        }

        public void SaveIndex()
        {
            _log.Debug("Total split time (s) = " + _totalsplits);
            _log.Debug("Total pages = " + _pageList.Count);
            List<int> keys = new List<int>(_cache.Keys());
            keys.Sort();
            // save index to disk
            foreach (var i in keys)
            {
                var p = _cache[i];
                if (p.isDirty)
                {
                    _index.SavePage(p);
                    p.isDirty = false;
                }
            }
            _index.BitmapFlush();
        }

        public void Shutdown()
        {
            // save page list
            _index.SavePageList(_pageList, _pageListDiskPages);
            // shutdown
            _index.Shutdown();
        }

        // FEATURE : bool includeDuplices, int start, int count)
        public IEnumerable<KeyValuePair<T, int>> Enumerate(T fromkey)
        {
            List<KeyValuePair<T, int>> list = new List<KeyValuePair<T, int>>();
            // enumerate
            PageInfo pi;
            Page<T> page = LoadPage(fromkey, out pi);
            T[] keys = page.tree.Keys();
            Array.Sort<T>(keys);

            int p = Array.BinarySearch<T>(keys, fromkey);
            for (int i = p; i < keys.Length; i++)
                list.Add(new KeyValuePair<T, int>(keys[i], page.tree[keys[i]].RecordNumber));

            while (page.RightPageNumber != -1)
            {
                page = LoadPage(page.RightPageNumber);
                keys = page.tree.Keys();
                Array.Sort<T>(keys);

                foreach (var k in keys)
                    list.Add(new KeyValuePair<T, int>(k, page.tree[k].RecordNumber));
            }

            return list;
        }

        public IEnumerable<int> GetDuplicates(T key)
        {
            PageInfo pi;
            Page<T> page = LoadPage(key, out pi);
            KeyInfo ki;
            bool ret = page.tree.TryGetValue(key, out ki);
            if (ret)
                // get duplicates
                if (ki.DuplicateBitmapNumber != -1)
                    return _index.GetDuplicatesRecordNumbers(ki.DuplicateBitmapNumber);

            return new List<int>();
        }

        public void SaveLastRecordNumber(int recnum)
        {
            _index.SaveLastRecordNumber(recnum);
        }

        public bool RemoveKey(T key)
        {
            PageInfo pi;
            Page<T> page = LoadPage(key, out pi);
            bool b = page.tree.Remove(key);
            if (b)
            {
                pi.UniqueCount--;
                // FEATURE : decrease dup count
            }
            return b;
        }

        public Statistics GetStatistics()
        {
            Statistics s = new Statistics();
            s.TotalSplitTime = _totalsplits;
            s.PageCount = _pageList.Count;

            return s;
        }

        #region [  P R I V A T E  ]

        private double _totalsplits = 0;
        private void SplitPage(Page<T> page)
        {
            // split the page
            DateTime dt = FastDateTime.Now;

            Page<T> newpage = new Page<T>(false);
            newpage.DiskPageNumber = _index.GetNewPageNumber();
            newpage.RightPageNumber = page.RightPageNumber;
            newpage.isDirty = true;
            page.RightPageNumber = newpage.DiskPageNumber;
            // get and sort keys
            T[] keys = page.tree.Keys();
            Array.Sort<T>(keys);
            // copy data to new 
            for (int i = keys.Length / 2; i < keys.Length; i++)
            {
                newpage.tree.Add(keys[i], page.tree[keys[i]]);
                // remove from old page
                page.tree.Remove(keys[i]);
            }
            // set the first key
            newpage.FirstKey = keys[keys.Length / 2];
            // set the first key refs
            _pageList.Remove(page.FirstKey);
            _pageList.Remove(keys[0]);
            // dup counts
            _pageList.Add(keys[0], new PageInfo(page.DiskPageNumber, page.tree.Count, 0));
            page.FirstKey = keys[0];
            // FEATURE : dup counts
            _pageList.Add(newpage.FirstKey, new PageInfo(newpage.DiskPageNumber, newpage.tree.Count, 0));
            _cache.Add(newpage.DiskPageNumber, newpage);

            _totalsplits += FastDateTime.Now.Subtract(dt).TotalSeconds;
        }

        private Page<T> LoadPage(T key, out PageInfo pageinfo)
        {
            int pagenum = -1;
            // find page in list of pages

            bool found = false;
            int pos = 0;
            if (key != null)
                pos = FindPageOrLowerPosition(key, ref found);
            pageinfo = _pageList.Values[pos];
            pagenum = pageinfo.PageNumber;

            Page<T> page;
            if (_cache.TryGetValue(pagenum, out page) == false)
            {
                //load page from disk
                page = _index.LoadPageFromPageNumber(pagenum);
                _cache.Add(pagenum, page);
            }
            return page;
        }

        private Page<T> LoadPage(int pagenum)
        {
            Page<T> page;
            if (_cache.TryGetValue(pagenum, out page) == false)
            {
                //load page from disk
                page = _index.LoadPageFromPageNumber(pagenum);
                _cache.Add(pagenum, page);
            }
            return page;
        }

        private void SaveDuplicate(T key, ref KeyInfo ki)
        {
            if (ki.DuplicateBitmapNumber == -1)
                ki.DuplicateBitmapNumber = _index.GetBitmapDuplaicateFreeRecordNumber();

            _index.SetBitmapDuplicate(ki.DuplicateBitmapNumber, ki.RecordNumber);
        }

        private int FindPageOrLowerPosition(T key, ref bool found)
        {
            if (_pageList.Count == 0)
                return 0;
            // binary search
            int lastlower = 0;
            int first = 0;
            int last = _pageList.Count - 1;
            int mid = 0;
            while (first <= last)
            {
                mid = (first + last) >> 1;
                T k = _pageList.Keys[mid];
                int compare = k.CompareTo(key);
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
        #endregion
    }
}
