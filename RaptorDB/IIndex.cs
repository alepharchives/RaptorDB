using System;
using System.Collections.Generic;
using System.Text;

namespace RaptorDB
{
    internal interface IIndex
    {
        bool InMemory { get; set; }
        bool Get(byte[] key, out int recnum);
        void Set(byte[] key, int recnum);
        void Commit();
        void Shutdown();
        IEnumerable<int> Enumerate(byte[] fromkey, int start, int count);
        long Count();
        IEnumerable<int> GetDuplicates(byte[] key);
        void SaveIndex();
    }
}
