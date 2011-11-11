﻿using System;
using System.Collections.Generic;
using System.Text;

namespace RaptorDB
{
    internal interface IIndex<T>
    {
        bool InMemory { get; set; }
        bool Get(T key, out int recnum);
        void Set(T key, int recnum);
        void Commit();
        void Shutdown();
        IEnumerable<int> Enumerate(T fromkey, int start, int count);
        long Count();
        IEnumerable<int> GetDuplicates(T key);
        void SaveIndex();
    }
}
