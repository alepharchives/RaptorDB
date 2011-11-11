using System;
using System.Collections.Generic;
using System.Text;

namespace RaptorDB
{
    #region [ internal classes ]
    internal class KeyPointer<T>
    {
        public KeyPointer(T key, int recno, int duppage)
        {
            RecordNum = recno;
            Key = key;
            DuplicatesRecNo = duppage;
        }

        public KeyPointer(T key, int recno)
        {
            RecordNum = recno;
            Key = key;
            DuplicatesRecNo = -1;
        }

        public T Key;
        public int RecordNum;
        public int DuplicatesRecNo = -1;

        public KeyPointer<T> Copy()
        {
            return new KeyPointer<T>(Key, RecordNum);
        }
    }

    #endregion

    internal class Node<T>
    {
        internal int DiskPageNumber = -1;
        internal List<KeyPointer<T>> ChildPointers = new List<KeyPointer<T>>();
        internal bool isLeafPage = false;
        internal bool isDirty = false;
        internal bool isDuplicatePage = false;
        internal bool isRootPage = false;
        internal int ParentPageNumber = -1;
        internal int RightPageNumber = -1;

        public Node(byte type, int parentpage, List<KeyPointer<T>> children, int diskpage, int rightpage)
        {
            DiskPageNumber = diskpage;
            ParentPageNumber = parentpage;
            RightPageNumber = rightpage;
            if ((type & 1) == 1)
                isLeafPage = true;
            if ((type & 2) == 2)
                isRootPage = true;
            if ((type & 4) == 4)
                isDuplicatePage = true;
            ChildPointers = children;
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
    }

    internal class Bucket<T>
    {
        internal int BucketNumber = -1;
        internal List<KeyPointer<T>> Pointers = new List<KeyPointer<T>>();

        internal int DiskPageNumber = -1;
        internal int NextPageNumber = -1;
        internal bool isDirty = false;
        internal bool isBucket = true;
        internal bool isOverflow = false;

        public Bucket(byte type, int bucketnumber, List<KeyPointer<T>> pointers, int diskpage, int nextpage)
        {
            DiskPageNumber = diskpage;
            BucketNumber = bucketnumber;
            NextPageNumber = nextpage;
            if ((type & 8) == 8)
                isBucket = true;
            if ((type & 16) == 16)
                isOverflow = true;
            Pointers = pointers;
        }

        public Bucket(int page)
        {
            DiskPageNumber = page;
        }

        public short Count
        {
            get { return (short)Pointers.Count; }
        }
    }
}
