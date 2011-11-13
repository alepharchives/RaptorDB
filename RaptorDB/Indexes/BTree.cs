using System;
using System.Collections.Generic;
using System.Text;
using System.IO;

namespace RaptorDB
{
    internal class BTree<T> : IIndex<T> where T : IRDBDataType<T>
    {
        private Node<T> _root = null;
        private IndexFile<T> _IndexFile = null;
        private SafeDictionary<int, Node<T>> _CachedNodes = new SafeDictionary<int, Node<T>>(Global.MaxItemsBeforeIndexing);
        private List<int> _LeafsToUnload = new List<int>();
        private short _Order = 4;
        private byte _MaxKeySize;
        private bool _allowDuplicates = false;
        private bool _InMemory = false;
        ILog log = LogManager.GetLogger(typeof(BTree<T>));

        public BTree(string indexfilename, byte maxkeysize, short nodeSize, bool allowDuplicates, int bucketcount)
        {
            _MaxKeySize = maxkeysize;
            _allowDuplicates = allowDuplicates;
            // make nodesize even
            if (nodeSize % 2 == 1)
                nodeSize++;
            _Order = nodeSize;

            _IndexFile = new IndexFile<T>(indexfilename, _MaxKeySize, _Order, bucketcount, INDEXTYPE.BTREE);
            Node<T> n = _IndexFile.GetRoot();
            if (n.isRootPage)
                _root = n;
            _CachedNodes.Add(n.DiskPageNumber, n);
            // get params from index file maxks, order
            _MaxKeySize = _IndexFile._maxKeySize;
            _Order = _IndexFile._PageNodeCount;
        }

        #region [   I I N D E X   ]

        public bool InMemory
        {
            get
            {
                return _InMemory;
            }
            set
            {
                _InMemory = value;
            }
        }

        public bool Get(T key, out int val)
        {
            T k = key;

            val = -1;
            Node<T> n = FindLeaf(_root, k);

            bool found = false;
            int pos = FindNodeOrLowerPosition(n, k, ref found);
            if (found)
            {
                val = n.ChildPointers[pos].RecordNum;
            }
            return found;
        }

        public void Set(T key, int val)
        {
            T k = key;

            if (_root == null)
            {
                _root = new Node<T>(this.GetNextPageNumber());
                DirtyNode(_root);
                _root.isRootPage = true;
                _root.DiskPageNumber = 0;
            }

            Node<T> nroot = null;
            Node<T> node = FindLeaf(_root, k);

            bool found = false;
            int lastlower = FindNodeOrLowerPosition(node, k, ref found);

            if (found)
            {
                int v = node.ChildPointers[lastlower].RecordNum;
                if (v != val)
                {
                    if (_allowDuplicates)
                        SaveDuplicate(node.ChildPointers[lastlower], v);
                    node.ChildPointers[lastlower].RecordNum = val;
                    DirtyNode(node);
                }
            }
            else
            {
                if (lastlower == -1)
                {
                    T oldkey = node.ChildPointers[0].Key;
                    node.ChildPointers.Insert(0, new KeyPointer<T>(k, val));
                    ReplaceParentKey(node, oldkey, k);
                }
                else
                {
                    lastlower++;
                    // add to list
                    if (lastlower < node.ChildPointers.Count)
                        node.ChildPointers.Insert(lastlower, new KeyPointer<T>(k, val));
                    else
                        node.ChildPointers.Add(new KeyPointer<T>(k, val));
                }
                DirtyNode(node);
                nroot = SplitNode(node);
            }

            // new root node
            if (nroot != null)
                _root = nroot;
        }

        public void Commit()
        {
            Node<T> n = null;
            if (_CachedNodes.TryGetValue(_root.DiskPageNumber, out n) == false)
                _CachedNodes.Add(_root.DiskPageNumber, _root);
            if (_InMemory == false)
                SaveIndex();
        }

        public void Shutdown()
        {
            log.Debug("Shutdown BTree");
            Commit();

            _IndexFile.Shutdown();

            _root = null;
        }

        public IEnumerable<int> Enumerate(T fromkey, int start, int count)
        {
            throw new NotImplementedException();
        }

        public long Count()
        {
            return _IndexFile.CountNodes(_root);
        }

        public IEnumerable<int> GetDuplicates(T key)
        {
            if (_allowDuplicates)
            {
                T k = key;
                Node<T> node = FindLeaf(_root, k);

                if (node != null)
                {
                    bool found = false;
                    int lastlower = FindNodeOrLowerPosition(node, k, ref found);

                    if (found)
                    {
                        int dp = node.ChildPointers[lastlower].DuplicatesRecNo;
                        if (dp != -1)
                        {
                            return _IndexFile.GetDuplicatesRecordNumbers(dp);
                        }
                    }
                }
            }
            return new List<int>();
        }

        public void SaveIndex()
        {
            if (_IndexFile.Commit(_CachedNodes.GetList()))
            {
                if (Global.FreeMemoryOnCommit)
                {
                    foreach (int i in _LeafsToUnload)
                        _CachedNodes.Remove(i);

                    _LeafsToUnload = new List<int>();

                    Node<T> r = null;
                    if (_CachedNodes.TryGetValue(_root.DiskPageNumber, out r) == false)
                        _CachedNodes.Add(_root.DiskPageNumber, _root);
                }
            }
        }
        #endregion

        #region [   P R I V A T E   M E T H O D S   ]

        private void SaveDuplicate(KeyPointer<T> key, int oldvalue)
        {
            if (key.DuplicatesRecNo == -1)
                key.DuplicatesRecNo = _IndexFile.GetBitmapDuplaicateFreeRecordNumber();

            _IndexFile.SetBitmapDuplicate(key.DuplicatesRecNo, oldvalue);
        }

        private int GetNextPageNumber()
        {
            return _IndexFile.GetNewPageNumber();
        }

        private Node<T> LoadNode(int number)
        {
            return LoadNode(number, false);
        }

        private Node<T> LoadNode(int number, bool skipcache)
        {
            if (number == -1)
                return _root;
            Node<T> n;
            if (_CachedNodes.TryGetValue(number, out n))
                return n;
            n = _IndexFile.LoadNodeFromPageNumber(number);
            if (skipcache == false)
            {
                _CachedNodes.Add(number, n);
                if (n.isLeafPage)
                    _LeafsToUnload.Add(number);
            }
            return n;
        }

        private void DirtyNode(Node<T> n)
        {
            if (n.isDirty)
                return;

            n.isDirty = true;
            Node<T> m = null;
            if (_CachedNodes.TryGetValue(n.DiskPageNumber, out m) == false)
                _CachedNodes.Add(n.DiskPageNumber, n);
        }

        private Node<T> SplitNode(Node<T> node)
        {
            if (node.ChildPointers.Count <= _Order)
                return null;

            Node<T> right = new Node<T>(this.GetNextPageNumber());
            DirtyNode(right);
            right.isLeafPage = node.isLeafPage;
            right.ParentPageNumber = node.ParentPageNumber;
            int mid = node.ChildPointers.Count / 2;
            KeyPointer<T>[] arrR = new KeyPointer<T>[mid + 1];
            KeyPointer<T>[] arrN = new KeyPointer<T>[mid];
            node.ChildPointers.CopyTo(mid, arrR, 0, mid + 1);
            node.ChildPointers.CopyTo(0, arrN, 0, mid);
            right.ChildPointers = new List<KeyPointer<T>>(arrR);
            node.ChildPointers = new List<KeyPointer<T>>(arrN);
            ReparentChildren(right);
            if (node.isLeafPage)
            {
                right.RightPageNumber = node.RightPageNumber;
                node.RightPageNumber = right.DiskPageNumber;
            }
            DirtyNode(node);
            if (node.isRootPage)
                return CreateNewRoot(node, right);
            else
            {
                Node<T> parent = this.LoadNode(node.ParentPageNumber);
                KeyPointer<T> kp = right.ChildPointers[0].Copy();
                kp.RecordNum = right.DiskPageNumber;
                bool found = false;
                int parentpos = FindNodeOrLowerPosition(parent, node.ChildPointers[0].Key, ref found);
                if (found)
                {
                    parentpos++;
                    if (parentpos < parent.ChildPointers.Count)
                        parent.ChildPointers.Insert(parentpos, kp);
                    else
                        parent.ChildPointers.Add(kp);
                    DirtyNode(parent);
                }
                else
                {
                    log.Error("should not be here, node not in parent");
                    throw new Exception("should not be here, node not in parent");
                }

                // cascade root split
                Node<T> newnode = SplitNode(parent);
                ReparentChildren(parent);

                return newnode;
            }
        }

        private Node<T> CreateNewRoot(Node<T> left, Node<T> right)
        {
            Node<T> newroot = new Node<T>(this.GetNextPageNumber());
            DirtyNode(newroot);
            newroot.isLeafPage = false;
            newroot.isRootPage = true;
            left.isRootPage = false;
            right.isRootPage = false;
            newroot.ChildPointers.Add(new KeyPointer<T>(left.ChildPointers[0].Key, left.DiskPageNumber));
            newroot.ChildPointers.Add(new KeyPointer<T>(right.ChildPointers[0].Key, right.DiskPageNumber));
            DirtyNode(left);
            DirtyNode(right);
            ReparentChildren(newroot);

            return newroot;
        }

        private Node<T> FindLeaf(Node<T> start, T key)
        {
            if (start.isLeafPage)
                return start;

            bool found = false;
            int pos = FindNodeOrLowerPosition(start, key, ref found);
            if (pos == -1) pos = 0;
            KeyPointer<T> ptr = start.ChildPointers[pos];

            Node<T> node = this.LoadNode(ptr.RecordNum);
            return FindLeaf(node, key);
        }

        private void ReplaceParentKey(Node<T> node, T oldkey, T key)
        {
            if (node.isRootPage == true)
                return;
            Node<T> parent = this.LoadNode(node.ParentPageNumber);
            bool found = false;
            int pos = FindNodeOrLowerPosition(parent, oldkey, ref found);
            if (found)
            {
                parent.ChildPointers[pos].Key = key;
                DirtyNode(parent);
                ReplaceParentKey(parent, oldkey, key);
            }
        }

        private void ReparentChildren(Node<T> node)
        {
            if (node.isLeafPage)
                return;
            foreach (KeyPointer<T> kp in node.ChildPointers)
            {
                Node<T> child = this.LoadNode(kp.RecordNum);
                child.ParentPageNumber = node.DiskPageNumber;
                DirtyNode(child);
            }
            DirtyNode(node);
        }

        private int FindNodeOrLowerPosition(Node<T> node, T key, ref bool found)
        {
            if (node.ChildPointers.Count == 0)
                return 0;
            // binary search
            int lastlower = -1;
            int first = 0;
            int last = node.ChildPointers.Count - 1;
            int mid = 0;
            while (first <= last)
            {
                mid = (first + last) >> 1;
                KeyPointer<T> k = node.ChildPointers[mid];
                int compare = k.Key.CompareTo(key);
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
