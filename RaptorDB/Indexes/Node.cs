using System;
using System.Collections.Generic;
using System.Text;

namespace RaptorDB
{
    #region [ internal classes ]
    internal class KeyPointer
    {
        public KeyPointer(bytearr key, int recno, int duppage)
        {
            RecordNum = recno;
            Key = key;
            DuplicatesRecNo = duppage;
        }

        public KeyPointer(bytearr key, int recno)
        {
            RecordNum = recno;
            Key = key;
            DuplicatesRecNo = -1;
        }

        public bytearr Key;
        public int RecordNum;
        public int DuplicatesRecNo = -1;

        //public override string ToString()
        //{
        //    return "" + Key;
        //}

        public KeyPointer Copy()
        {
            return new KeyPointer(Key, RecordNum);
        }
    }

    internal struct bytearr
    {
        public bytearr(byte[] key)
        {
            val = key;
        }
        public byte[] val;

        //public override string ToString()
        //{
        //    return "" + val[0];
        //}
        public override int GetHashCode()
        {
            int result = 17;
            foreach (byte b in val)
            {
                result = result * 31 + b;
            }
            return result;
        }
    }
    #endregion

    internal class Node
    {
        internal int DiskPageNumber = -1;
        internal List<KeyPointer> ChildPointers = new List<KeyPointer>();
        internal bool isLeafPage = false;
        internal bool isDirty = false;
        internal bool isDuplicatePage = false;
        internal bool isRootPage = false;  
        //internal List<int> Duplicates = new List<int>();
        internal int ParentPageNumber = -1;
        internal int RightPageNumber = -1;

        public Node(byte type, int parentpage, List<KeyPointer> children, //List<int> duplicates, 
            int diskpage, int rightpage)
        {
            DiskPageNumber = diskpage;
            ParentPageNumber = parentpage;
            RightPageNumber = rightpage;
            if ((type & 1) == 1)
                isLeafPage = true;
            if ((type & 2) == 2)
                isRootPage = true;
            if((type & 4) == 4)
                isDuplicatePage =true;
            ChildPointers = children;
            //Duplicates = duplicates;
        }

        public Node(int diskpage)
        {
            isLeafPage = true;
            DiskPageNumber = diskpage;
            isRootPage = false;
        }

        public short Count
        {
            get { return (short)ChildPointers.Count; }
        }

        //public override string ToString()
        //{
        //    return "childs=" + ChildPointers.Count + " page=" + diskPageNumber + " isleaf=" + isLeaf;
        //}
    }

    internal class Bucket
    {
        internal int BucketNumber = -1;
        internal List<KeyPointer> Pointers = new List<KeyPointer>();
        //internal List<int> Duplicates = new List<int>();

        internal int DiskPageNumber = -1;
        internal int NextPageNumber = -1;
        internal bool isDirty = false;
        internal bool isBucket = true;
        internal bool isOverflow = false;

        public Bucket(byte type, int bucketnumber, List<KeyPointer> pointers, // List<int> duplicates,
            int diskpage, int nextpage)
        {
            DiskPageNumber = diskpage;
            BucketNumber = bucketnumber;
            NextPageNumber = nextpage;
            if ((type & 8) == 8)
                isBucket = true;
            if ((type & 16) == 16)
                isOverflow = true;
            Pointers = pointers;
            //Duplicates = duplicates;
        }

        public Bucket(int page)
        {
            DiskPageNumber = page;
        }

        public short Count
        {
            get { return (short)Pointers.Count; }
        }

        //public override string ToString()
        //{
        //    return "count = " + Pointers.Count;
        //}
    }
}
