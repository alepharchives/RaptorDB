using System;
using System.Collections.Generic;
using System.Text;
using System.IO;
using System.Threading;
using System.Collections;

namespace RaptorDB
{
    public class RaptorDB<T> : KeyStore<T> where T : IComparable<T>
    {
        public RaptorDB(string Filename, byte MaxKeySize, bool AllowDuplicateKeys) : base(Filename, MaxKeySize, AllowDuplicateKeys)
        {
        }

        public RaptorDB(string Filename, bool AllowDuplicateKeys) : base( Filename, AllowDuplicateKeys)
        {
        }

        public new static RaptorDB<T> Open(string Filename, bool AllowDuplicateKeys)
        {
            return new RaptorDB<T>(Filename, AllowDuplicateKeys);
        }

        public new static RaptorDB<T> Open(string Filename, byte MaxKeySize, bool AllowDuplicateKeys)
        {
            return new RaptorDB<T>(Filename, MaxKeySize, AllowDuplicateKeys);
        }
    }
}
