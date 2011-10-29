using System;
using System.Collections.Generic;
using System.Text;
using System.IO;

namespace RaptorDB
{
    internal class BTree : IIndex
    {
        private Node root = null;
        private IndexFile _IndexFile = null;
        private Dictionary<int, Node> CachedNodes = new Dictionary<int, Node>(Global.MaxItemsBeforeIndexing);
        private short _Order = 4;
        private byte _MaxKeySize;
        private bool _allowDuplicates = false;
        private bool _InMemory = false;

        public BTree(string indexfilename, byte maxkeysize, short nodeSize, bool allowDuplicates, int bucketcount)
        {
            _MaxKeySize = maxkeysize;
            _allowDuplicates = allowDuplicates;
            // make nodesize even
            if (nodeSize % 2 == 1)
                nodeSize++;
            _Order = nodeSize;

            _IndexFile = new IndexFile(indexfilename, _MaxKeySize, _Order, bucketcount, INDEXTYPE.BTREE);
            Node n = _IndexFile.GetRoot();
            if (n.isRootPage)
                root = n;
            CachedNodes.Add(n.DiskPageNumber, n);
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

        public bool Get(byte[] key, out int val)
        {
            bytearr k = new bytearr(key);

            val = -1;
            Node n = FindLeaf(root, k);

            bool found = false;
            int pos = FindNodeOrLowerPosition(n, k, ref found);
            if (found)
            {
                val = n.ChildPointers[pos].RecordNum;
            }
            return found;
        }

        public void Set(byte[] key, int val)
        {
            bytearr k = new bytearr(key);

            if (root == null)
            {
                root = new Node(this.GetNextPageNumber());
                DirtyNode(root);
                root.isRootPage = true;
                root.DiskPageNumber = 0;
            }

            Node nroot = null;
            Node node = FindLeaf(root, k);

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
                    bytearr oldkey = node.ChildPointers[0].Key;
                    node.ChildPointers.Insert(0, new KeyPointer(k, val));
                    ReplaceParentKey(node, oldkey, k);
                }
                else
                {
                    lastlower++;
                    // add to list
                    if (lastlower < node.ChildPointers.Count)
                        node.ChildPointers.Insert(lastlower, new KeyPointer(k, val));
                    else
                        node.ChildPointers.Add(new KeyPointer(k, val));
                }
                DirtyNode(node);
                nroot = SplitNode(node);
            }

            // new root node
            if (nroot != null)
                root = nroot;
        }

        public void Commit()
        {
            if (CachedNodes.ContainsKey(root.DiskPageNumber) == false)
                CachedNodes.Add(root.DiskPageNumber, root);
            if (_InMemory == false)
                SaveIndex();
        }

        public void Shutdown()
        {
            Commit();
            _IndexFile.Shutdown();

            root = null;
        }

        public IEnumerable<int> Enumerate(byte[] fromkey, int start, int count)
        {
            throw new NotImplementedException();
        }

        public long Count()
        {
            return _IndexFile.CountNodes(root);
        }

        public IEnumerable<int> GetDuplicates(byte[] key)
        {
            if (_allowDuplicates)
            {
                bytearr k = new bytearr(key);
                Node node = FindLeaf(root, k);

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
            if (_IndexFile.Commit(CachedNodes))
            {
                Dictionary<int, Node> ar = new Dictionary<int, Node>(Global.MaxItemsBeforeIndexing);
                
                foreach (var kv in CachedNodes)
                {
                    if (kv.Value.isLeafPage == false)
                        ar.Add(kv.Key, kv.Value);
                }
                //CachedNodes = new Dictionary<int, Node>(Global.MaxItemsBeforeIndexing);
                CachedNodes = ar;
                Node r = null;
                if (CachedNodes.TryGetValue(root.DiskPageNumber, out r) == false)
                    CachedNodes.Add(root.DiskPageNumber, root);
            }
        }
        #endregion

        #region [   P R I V A T E   M E T H O D S   ]

        private void SaveDuplicate(KeyPointer key, int oldvalue)
        {
            if (key.DuplicatesRecNo == -1)
                key.DuplicatesRecNo = _IndexFile.GetBitmapDuplaicateFreeRecordNumber();

            _IndexFile.SetBitmapDuplicate(key.DuplicatesRecNo, oldvalue);
        }

        private int GetNextPageNumber()
        {
            return _IndexFile.GetNewPageNumber();
        }

        private Node LoadNode(int number)
        {
            return LoadNode(number, false);
        }

        private Node LoadNode(int number, bool skipcache)
        {
            if (number == -1)
                return root;
            Node n;
            if (CachedNodes.TryGetValue(number, out n))
                return n;
            n = _IndexFile.LoadNodeFromPageNumber(number);
            if (skipcache == false)
                CachedNodes.Add(number, n);
            return n;
        }

        private void DirtyNode(Node n)
        {
            if (n.isDirty)
                return;

            n.isDirty = true;
            if (CachedNodes.ContainsKey(n.DiskPageNumber) == false)
                CachedNodes.Add(n.DiskPageNumber, n);
        }

        private Node SplitNode(Node node)
        {
            if (node.ChildPointers.Count <= _Order)
                return null;

            Node right = new Node(this.GetNextPageNumber());
            DirtyNode(right);
            right.isLeafPage = node.isLeafPage;
            right.ParentPageNumber = node.ParentPageNumber;
            int mid = node.ChildPointers.Count / 2;
            KeyPointer[] arrR = new KeyPointer[mid + 1];
            KeyPointer[] arrN = new KeyPointer[mid];
            node.ChildPointers.CopyTo(mid, arrR, 0, mid + 1);
            node.ChildPointers.CopyTo(0, arrN, 0, mid);
            right.ChildPointers = new List<KeyPointer>(arrR);
            node.ChildPointers = new List<KeyPointer>(arrN);
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
                Node parent = this.LoadNode(node.ParentPageNumber);
                KeyPointer kp = right.ChildPointers[0].Copy();
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
                    throw new Exception("should not be here, node not in parent");

                // cascade root split
                Node newnode = SplitNode(parent);
                ReparentChildren(parent);

                return newnode;
            }
        }

        private Node CreateNewRoot(Node left, Node right)
        {
            Node newroot = new Node(this.GetNextPageNumber());
            DirtyNode(newroot);
            newroot.isLeafPage = false;
            newroot.isRootPage = true;
            left.isRootPage = false;
            right.isRootPage = false;
            newroot.ChildPointers.Add(new KeyPointer(left.ChildPointers[0].Key, left.DiskPageNumber));
            newroot.ChildPointers.Add(new KeyPointer(right.ChildPointers[0].Key, right.DiskPageNumber));
            DirtyNode(left);
            DirtyNode(right);
            ReparentChildren(newroot);

            return newroot;
        }

        private Node FindLeaf(Node start, bytearr key)
        {
            if (start.isLeafPage)
                return start;

            bool found = false;
            int pos = FindNodeOrLowerPosition(start, key, ref found);
            if (pos == -1) pos = 0;
            KeyPointer ptr = start.ChildPointers[pos];

            Node node = this.LoadNode(ptr.RecordNum);
            return FindLeaf(node, key);
        }

        private void ReplaceParentKey(Node node, bytearr oldkey, bytearr key)
        {
            if (node.isRootPage == true)
                return;
            Node parent = this.LoadNode(node.ParentPageNumber);
            bool found = false;
            int pos = FindNodeOrLowerPosition(parent, oldkey, ref found);
            if (found)
            {
                parent.ChildPointers[pos].Key = key;
                DirtyNode(parent);
                ReplaceParentKey(parent, oldkey, key);
            }
        }

        private void ReparentChildren(Node node)
        {
            if (node.isLeafPage)
                return;
            foreach (KeyPointer kp in node.ChildPointers)
            {
                Node child = this.LoadNode(kp.RecordNum);
                child.ParentPageNumber = node.DiskPageNumber;
                DirtyNode(child);
            }
            DirtyNode(node);
        }

        private int FindNodeOrLowerPosition(Node node, bytearr key, ref bool found)
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
                KeyPointer k = node.ChildPointers[mid];
                int compare = Helper.Compare(k.Key, key);
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
