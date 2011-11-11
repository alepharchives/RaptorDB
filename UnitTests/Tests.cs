using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using NUnit.Framework;
using RaptorDB;
using System.IO;

namespace UnitTests
{
    [TestFixture]
    public class Tests
    {
        static string string1kb =
            // ~1kb
            "{\"$type\":\"BizFX.TestApp.Entity.Customer, BizFX.TestApp.Entity, Version=1.0.0.0, Culture=neutral, PublicKeyToken=426204062733118a\",\"Address\":null,\"Code\":null,\"Phone\":null,\"email\":null,\"Mobile\":null,\"ContactName\":null,\"Comments\":null,\"GUID\":\"iNIaPond7k6McmSStz14kA==\",\"BaseInfo\":{\"$type\":\"BizFX.Entity.BaseInfo, BizFX.Entity, Version=2.0.0.0, Culture=neutral, PublicKeyToken=e5d192f5e46064af\",\"RevisionNumber\":0,\"CreateDate\":\"2011-04-06 10:16:50\",\"SkipSync\":false,\"SkipDocs\":false,\"SkipRunning\":false,\"DeleteRevisions\":false,\"AssemblyFilename\":\"BizFX.TestApp.Entity.Customer, BizFX.TestApp.Entity, Version=1.0.0.0, Culture=neutral, PublicKeyToken=426204062733118a\",\"TypeName\":\"BizFX.TestApp.Entity.Customer\"},\"SecurityInfo\":{\"$type\":\"BizFX.Entity.SecurityInfo, BizFX.Entity, Version=2.0.0.0, Culture=neutral, PublicKeyToken=e5d192f5e46064af\",\"WinUserName\":\"\",\"AppUserName\":\"\",\"GUID\":\"FAfCsxxJOUuLJITZj005Ow==\",\"LoginName\":\"\",\"UserName\":\"\",\"MachineName\":\"\",\"UserDomainName\":\"\"},\"Description\":\"Base entity description.\",\"Name\":\"BaseEntity\"}";

        static string string100b =
            // ~100 bytes
            "{\"$type\":\"BizFX.TestApp.Entity.Customer, BizFX.TestApp.Entity, Version=1.0.0.0, Culture=neutral, PublicKeyToken=426204062733118a\"";

        //[Test]
        //public void String_As_Key_btree()
        //{
        //    string_as_key(INDEXTYPE.BTREE);
        //}


        //[Test]
        //public void String_As_Key_hash()
        //{
        //    string_as_key(INDEXTYPE.HASH);
        //}

        //private static void string_as_key(INDEXTYPE type)
        //{
        //    int count = 40000;
        //    Console.WriteLine("Count = " + count);
        //    RaptorDB.RaptorDB rap = RaptorDB.RaptorDB.Open("docs\\strings" + type, 30, true, type);
        //    rap.IndexingTimerSeconds = 1;
        //    rap.InMemoryIndex = true;
        //    string key = "some very long string";

        //    for (int i = 0; i < count; i++)
        //    {
        //        string ss = key + i.ToString("000000");
        //        rap.Set(key, ss);
        //    }
        //    //System.Threading.Thread.Sleep(5000);
        //    rap.SaveIndex(true);
        //    int j = 0;
        //    for (int i = 0; i < count; i++)
        //    {
        //        string ss = key + i.ToString("000000");
        //        byte[] bb = null;
        //        if (rap.Get(key, out bb) == false)
        //            j++;// Console.WriteLine("error");
        //    }
        //    Console.WriteLine("Error count = " + j);
        //    Assert.AreEqual(j, 0);
        //}

        [Test]
        public void Duplicate_Guid_Key_btree()
        {
            duplicate_guid_key(INDEXTYPE.BTREE, true);
        }


        [Test]
        public void Duplicate_Guid_Key_hash()
        {
            duplicate_guid_key(INDEXTYPE.HASH, true);
        }

        private static void duplicate_guid_key(INDEXTYPE type, bool allowdups)
        {
            byte[] bb = System.Text.Encoding.UTF8.GetBytes(string1kb);
            RaptorDB.RaptorDB<rdbByteArray> rap = RaptorDB.RaptorDB<rdbByteArray>.Open("docs\\duplicates" + type, 16, allowdups, type);
            Guid g = Guid.NewGuid();
            Console.WriteLine("saving...");
            rap.Set(new rdbByteArray(Guid.NewGuid().ToByteArray()), bb);
            for (int i = 0; i < 20000; i++)
            {
                rap.Set(new rdbByteArray(g.ToByteArray()), bb);
            }
            rap.SaveIndex();
            foreach (var l in rap.GetDuplicates(new rdbByteArray(g.ToByteArray())))
            {
                byte[] dup = rap.FetchDuplicate(l);
                int i = dup.Length;
                Assert.AreEqual(i, bb.Length);
            }

            //foreach (var b in rap.EnumerateStorageFile())
            //{
            //    g = new Guid(b.Key);
            //    string sss = System.Text.Encoding.UTF8.GetString(b.Value);
            //    Console.Write(g);
            //}
        }


        [Test]
        public static void Dave_Killer_Test()
        {
            var db = new RaptorDB.RaptorDBGuid("c:\\RaptorDbTest\\RawFileOne");//16, true, INDEXTYPE.BTREE);
            //db.InMemoryIndex = true;
            //db.IndexingTimerSeconds = 1000;
            var guids = new List<Guid>();
            var sw = new System.Diagnostics.Stopwatch();
            const int trials = 1000000;
            for (int x = 0; x < trials; x++)
            {
                guids.Add(Guid.NewGuid());
            }
            sw.Start();
            for (int x = 0; x < trials; x++)
            {
                var id = guids[x];
                var key = id.ToByteArray();
                var value = id.ToString();
                db.Set(id, Encoding.UTF8.GetBytes(value));
            }
            sw.Stop();
            Console.Out.WriteLine("\n\n\nWriting {0} items took: {1}", trials, sw.Elapsed);
            var writeSpeed = trials * 1000.0d / sw.ElapsedMilliseconds;
            Console.Out.WriteLine("\nAverage write speed: {0} items/second.", writeSpeed);
            Console.Out.Write("Saving indexes...");
            sw.Reset();
            sw.Start();
            db.SaveIndex();
            sw.Stop();
            Console.Out.WriteLine("\n\nSaving indices took {0} ms", sw.ElapsedMilliseconds);
            sw.Reset(); sw.Start();
            int readCount = 0;
            byte[] valBytes;
            for (int x = 0; x < trials; x++)
            {
                var id = guids[x];

                if (db.Get(id, out valBytes))
                {
                    readCount++;
                }
            }
            sw.Stop();
            Console.Out.WriteLine("\n\n\nSuccessfully read {0} of {1} items took: {2}...", readCount, trials, sw.Elapsed);
            var readSpeed = readCount * 1000.0d / sw.ElapsedMilliseconds;
            Console.Out.WriteLine("\nAverage index hit/read speed: {0:0.##} items/second.", readSpeed);
            Console.WriteLine("\n\n\nAdding new values at the same keys (Duplicates enabled)...");
            sw.Reset();
            var newValues = new List<string>(trials);
            for (int x = 0; x < trials; x++)
            {
                newValues.Add(guids[x] + "ABC");
            }
            sw.Start();
            for (int x = 0; x < trials; x++)
            {
                db.Set(
                    //Guid.NewGuid()
                     guids[x]
                    , Encoding.UTF8.GetBytes(newValues[x]));
            }
            sw.Stop();
            Console.Out.WriteLine("Set {0} duplicate keys in {1}", trials, sw.Elapsed);
            var setDuplicateSpeed = trials * 1000.0d / sw.ElapsedMilliseconds;
            Console.Out.WriteLine("Average 'set duplicate key value speed' = {0} items/second.", setDuplicateSpeed);
            /* Again I MUST wait for the indexing to finish or I'm toast */
            sw.Reset(); sw.Start();

            db.SaveIndex();
            sw.Stop();
            Console.Out.WriteLine("\n\nSaving indices took {0} ms", sw.ElapsedMilliseconds);

            sw.Reset();
            sw.Start();
            Console.Out.WriteLine("\nEnumerating sets of 2 duplicates...");
            int duplicateCount = 0;
            for (int x = 0; x < trials; x++)
            {
                //try
                {
                    var ints = db.GetDuplicates(guids[x]);
                    duplicateCount += ints.Count();
                }
                //catch (Exception ex)
                //{
                //    Console.WriteLine("" + ex);
                //}
            }
            sw.Stop();
            Console.WriteLine("\n\n\nSuccessfully enumerated {0} items and found {1} duplicates in {2}", trials, duplicateCount, sw.Elapsed);
            var duplicateSpeed = duplicateCount * 1000.0d / sw.ElapsedMilliseconds;
            Console.WriteLine("\nSpeed of enumerating duplicates: {0} items/second.", duplicateSpeed);
        }


        [Test]
        public static void Duplicates_Set_and_Fetch_btree()
        {
            var db = new RaptorDB.RaptorDBGuid("c:\\RaptorDbTest\\duptestfetch");//, 16, true, INDEXTYPE.BTREE);
            //db.InMemoryIndex = true;
            //db.IndexingTimerSeconds = 1000;
            int guidcount = 1000;
            int dupcount = 100;
            var guids = new List<Guid>();
            for (int i = 0; i < guidcount; i++)
                guids.Add(Guid.NewGuid());

            foreach (Guid g in guids)
            {
                for (int i = 0; i < dupcount; i++)
                {
                    string s = "" + g + " " + i;
                    db.Set(g, Encoding.UTF8.GetBytes(s));
                }
            }
            db.SaveIndex();

            foreach (Guid g in guids)
            {
                int j = 0;
                foreach (int i in db.GetDuplicates(g.ToByteArray()))
                {
                    byte[] b = db.FetchDuplicate(i);
                    string s = Encoding.UTF8.GetString(b);
                    if (s.StartsWith(g.ToString()) == false)
                        Console.WriteLine("guid not correct = " + g + " returned = " + s);
                    else
                        j++;
                }
                if (j < dupcount - 1)
                {
                    Console.WriteLine("" + g + " count = " + j);
                    Assert.Fail();
                }
                else
                    Console.WriteLine("" + g + " = OK");
            }

            db.Shutdown();
        }

        [Test]
        public static void One_Million_Set_Get_BTREE_flush()
        {
            Set_Get("1millionflush", INDEXTYPE.BTREE, 1000000, true, true, false);
        }

        [Test]
        public static void One_Million_Set_Get_BTREE()
        {
            Set_Get("1million", INDEXTYPE.BTREE, 1000000, true, false, false);
        }

        [Test]
        public static void Twenty_Million_Set_Get_BTREE()
        {
            Console.WriteLine("Twenty million insert test");
            Console.WriteLine("This test will use a peak of 1.6Gb ram for guid keys");
            Console.WriteLine("This test will run for about 16 minutes depending on your hardware");
            Set_Get("20million", INDEXTYPE.BTREE, 20 * 1000000, true, true, false);
        }

        [Test]
        public static void Ten_Million_Set_Get_BTREE()
        {
            Console.WriteLine("Twenty million insert test");
            Console.WriteLine("This test will use a peak of 1.2Gb ram for guid keys");
            Console.WriteLine("This test will run for about 10 minutes depending on your hardware");
            Set_Get("10million", INDEXTYPE.BTREE, 10 * 1000000, true, true, false);
        }

        //[Test]
        //public static void OneHundred_Million_Set_Get_BTREE()
        //{
        //    Console.WriteLine("Two hundred million insert test");
        //    Console.WriteLine("This test will use a peak of 8Gb ram for guid keys");
        //    Console.WriteLine("This test will run for about 1.5 hour depending on your hardware");
        //    Set_Get("100million", INDEXTYPE.BTREE, 100 * 1000000, true, true, true);
        //}

        [Test]
        public static void One_Million_Set_Get_HASH()
        {
            Set_Get("1million", INDEXTYPE.HASH, 1000000, true, false, false);
        }

        [Test]
        public static void One_Million_Set_Get_HASH_flush()
        {
            Set_Get("1millionflush", INDEXTYPE.HASH, 1000000, true, true, false);
        }

        private static void Set_Get(string fname, INDEXTYPE type, int count, bool inmem, bool flush, bool skiplist)
        {
            Console.WriteLine("One million test on " + type);
            var db = RaptorDB.RaptorDB<rdbByteArray>.Open("c:\\RaptorDbTest\\" + fname + type, 16, false, type);
            db.InMemoryIndex = inmem;
            //db.IndexingTimerSeconds = 1000;

            var guids = new List<Guid>();
            if (skiplist == false)
            {
                Console.Write("Building guid list...");
                for (int i = 0; i < count; i++)
                    guids.Add(Guid.NewGuid());
            }
            Console.WriteLine("done");
            DateTime dt = DateTime.Now;
            int c = 0;
            if (skiplist == false)
            {
                foreach (Guid g in guids)
                {
                    string s = "" + g;
                    db.Set(new rdbByteArray(g.ToByteArray()), Encoding.Unicode.GetBytes(s));
                    c++;
                    if (c % 10000 == 0)
                        Console.Write(".");
                    if (c % 100000 == 0)
                        Console.WriteLine("time = " + DateTime.Now.Subtract(dt).TotalSeconds);
                }
            }
            else
            {
                for (int k = 0; k < count; k++)
                {
                    Guid g = Guid.NewGuid();
                    string s = "" + g;
                    db.Set(new rdbByteArray(g.ToByteArray()), Encoding.Unicode.GetBytes(s));
                    c++;
                    if (c % 10000 == 0)
                        Console.Write(".");
                    if (c % 100000 == 0)
                        Console.WriteLine("time = " + DateTime.Now.Subtract(dt).TotalSeconds);
                }
            }
            Console.WriteLine("Flushing index...");
            db.SaveIndex();
            Console.WriteLine(count.ToString("#,#") + " save total time = " + DateTime.Now.Subtract(dt).TotalSeconds);
            dt = DateTime.Now;
            if (skiplist == false)
            {
                //db.Shutdown();
                db.Dispose();
                db = null;
                db = RaptorDB.RaptorDB<rdbByteArray>.Open("c:\\RaptorDbTest\\" + fname + type, 16, false, type);
            }
            GC.Collect(2);
            int notfound = 0;
            c = 0;
            if (skiplist == false)
            {
                foreach (Guid g in guids)
                {
                    byte[] val;
                    if (db.Get(new rdbByteArray(g.ToByteArray()), out val))
                    {
                        string s = Encoding.Unicode.GetString(val);
                        if (s.Equals("" + g) == false)
                            Assert.Fail("data does not match " + g);
                    }
                    else
                    {
                        notfound++;
                        //Assert.Fail("item not found " + g);
                    }
                    c++;
                    if (c % 100000 == 0)
                        Console.Write(".");
                }
                if (notfound > 0)
                {
                    Console.WriteLine("items not found  = " + notfound);
                    Assert.Fail("items not found");
                }
                Console.WriteLine("\r\nfetch total time = " + DateTime.Now.Subtract(dt).TotalSeconds);
            }
            Console.WriteLine("ALL DONE OK");
            //db.Shutdown();
            db.Dispose();
            db = null;
        }

        //[Test]
        //public static void WAH_Bitarray_test()
        //{
        //    WAHBitArray ba = new WAHBitArray();

        //    Random r = new Random();

        //    // ----------------------------------------------------------
        //    Console.Write("Random bits set...");
        //    for (int i = 0; i < 100000; i++)
        //    {
        //        if (r.Next(200) == 0)
        //        {
        //            ba.Set(i, true);
        //        }
        //    }
        //    uint[] ui = ba.GetCompressed();
        //    WAHBitArray b2 = new WAHBitArray(ba.UsingIndexes ? WAHBitArray.TYPE.Indexes : WAHBitArray.TYPE.Compressed_WAH, ui);

        //    WAHBitArray result = b2.Xor(ba);
        //    Assert.AreEqual(0, result.CountOnes());
        //    Console.WriteLine(" done");



        //    // ----------------------------------------------------------
        //    Console.Write("All 1 set...");
        //    ba = new WAHBitArray();
        //    for (int i = 0; i < 100000; i++)
        //        ba.Set(i, true);
        //    ui = ba.GetCompressed();
        //    b2 = new WAHBitArray(ba.UsingIndexes ? WAHBitArray.TYPE.Indexes : WAHBitArray.TYPE.Compressed_WAH, ui);

        //    result = b2.Xor(ba);
        //    Assert.AreEqual(0, result.CountOnes());
        //    Console.WriteLine(" done");



        //    // ----------------------------------------------------------
        //    Console.Write("Alternate 1 set...");
        //    ba = new WAHBitArray();
        //    for (int i = 0; i < 100000; i++)
        //    {
        //        if (i % 2 == 0)
        //            ba.Set(i, true);
        //    }
        //    ui = ba.GetCompressed();
        //    b2 = new WAHBitArray(ba.UsingIndexes ? WAHBitArray.TYPE.Indexes : WAHBitArray.TYPE.Compressed_WAH, ui);

        //    result = b2.Xor(ba);
        //    Assert.AreEqual(0, result.CountOnes());
        //    Console.WriteLine(" done");



        //    // ----------------------------------------------------------
        //    Console.Write("Alternate 1 xor not self set...");
        //    ba = new WAHBitArray();
        //    for (int i = 0; i < 100000; i++)
        //    {
        //        if (i % 2 == 0)
        //            ba.Set(i, true);
        //    }
        //    ui = ba.GetCompressed();
        //    b2 = new WAHBitArray(ba.UsingIndexes ? WAHBitArray.TYPE.Indexes : WAHBitArray.TYPE.Compressed_WAH, ui);

        //    result = b2.Xor(ba.Not());
        //    Assert.AreEqual(100000, result.CountOnes());
        //    Console.WriteLine(" done");



        //    // ----------------------------------------------------------
        //    Console.Write("half 1 set...");
        //    ba = new WAHBitArray();
        //    for (int i = 50000; i < 100000; i++)
        //    {
        //        ba.Set(i, true);
        //    }
        //    ui = ba.GetCompressed();
        //    b2 = new WAHBitArray(ba.UsingIndexes ? WAHBitArray.TYPE.Indexes : WAHBitArray.TYPE.Compressed_WAH, ui);

        //    result = b2.Xor(ba);
        //    Assert.AreEqual(0, result.CountOnes());
        //    Console.WriteLine(" done");



        //    // ----------------------------------------------------------
        //    Console.Write("other half 1 set...");
        //    ba = new WAHBitArray();
        //    for (int i = 0; i < 50000; i++)
        //    {
        //        ba.Set(i, true);
        //    }
        //    ba.Set(100000, true);
        //    ui = ba.GetCompressed();
        //    b2 = new WAHBitArray(ba.UsingIndexes ? WAHBitArray.TYPE.Indexes : WAHBitArray.TYPE.Compressed_WAH, ui);

        //    result = b2.Xor(ba);
        //    Assert.AreEqual(0, result.CountOnes());
        //    Console.WriteLine(" done");
        //}

        //[Test]
        //public static void WAH_Multiple_duplicates_test()
        //{
        //    for (int i = 0; i < 100; i++)
        //    {
        //        Console.WriteLine("i = " + i);
        //        WAHBitArray ba = new WAHBitArray();
        //        for (int j = 0; j < 1000; j++)
        //        {
        //            ba.Set(i * 1000 + j, true);
        //        }
        //        uint[] ui = ba.GetCompressed();
        //        WAHBitArray b2 = new WAHBitArray(ba.UsingIndexes ? WAHBitArray.TYPE.Indexes : WAHBitArray.TYPE.Compressed_WAH, ui);

        //        WAHBitArray result = b2.Xor(ba);
        //        Assert.AreEqual(0, result.CountOnes());
        //        Console.WriteLine(" done");
        //    }
        //}


        [Test]
        public static void RaptodDBString_test()
        {
            Console.WriteLine("unlimited key size test");
            var rap = new RaptorDBString(@"c:\raptordbtest\longstringkey", false);

            for (int i = 0; i < 100000; i++)
            {
                rap.Set(string1kb + i, i.ToString());
            }
            Console.WriteLine("flushing index...");

            rap.SaveIndex();

            Console.WriteLine("fetching values...");

            for (int i = 0; i < 100000; i++)
            {
                string str = "";
                if (rap.Get(string1kb + i, out str))
                {
                    if (i.ToString() != str)
                        Assert.Fail("value does not match");
                }
                else
                    Assert.Fail("value not found");
            }
            rap.Shutdown();
        }


        //[Test]
        //public static void Hash_Guid_conflict_test()
        //{
        //    Console.WriteLine("This test will fail if the hash function output conflicts on two different inputs.");
        //    int count = 40 * 1000000;
        //    Console.WriteLine("test count = " + count.ToString("#,#"));
        //    Dictionary<Guid, uint> dic = new Dictionary<Guid, uint>(count);
        //    RaptorDB.MurmurHash2Unsafe hc = new MurmurHash2Unsafe();
        //    //Guid gg = Guid.NewGuid();
        //    //dic.Add(gg, 1);
        //    //dic.Add(gg, 2);
        //    for (int i = 0; i < count; i++)
        //    {
        //        Guid g = Guid.NewGuid();
        //        uint h = hc.Hash(g.TordbByteArrayay());

        //        dic.Add(g, h);
        //    }
        //}
        [Test]
        public static void Twenty_Million_Optimized_GUID()
        {
            int count = 20 * 1000000;
            Optimized_GUID(count);
        }

        [Test]
        public static void Ten_Million_Optimized_GUID()
        {
            int count = 10 * 1000000;
            Optimized_GUID(count);
        }

        private static void Optimized_GUID(int count)
        {
            Console.WriteLine("testing : " + count.ToString("#,#"));
            var db = new RaptorDBGuid("c:\\RaptorDbTest\\" + count);

            var guids = new List<Guid>();
            Console.Write("Building guid list...");
            for (int i = 0; i < count; i++)
                guids.Add(Guid.NewGuid());
            Console.WriteLine("done");
            FileStream fs = new FileStream("c:\\RaptorDbTest\\guids", FileMode.Create);
            for (int i = 0; i < count; i++)
                fs.Write(guids[i].ToByteArray(), 0, 16);
            fs.Flush();
            fs.Close();
            DateTime dt = DateTime.Now;
            int c = 0;
            foreach (Guid g in guids)
            {
                string s = "" + g;
                db.Set(g, Encoding.Unicode.GetBytes(s));
                c++;
                if (c % 10000 == 0)
                    Console.Write(".");
                if (c % 100000 == 0)
                    Console.WriteLine("time = " + DateTime.Now.Subtract(dt).TotalSeconds);
            }
            Console.WriteLine("Flushing index...");
            db.SaveIndex();
            Console.WriteLine(count.ToString("#,#") + " save total time = " + DateTime.Now.Subtract(dt).TotalSeconds);
            dt = DateTime.Now;
            GC.Collect(2);
            int notfound = 0;
            c = 0;
            foreach (Guid g in guids)
            {
                byte[] val;
                if (db.Get(g, out val))
                {
                    string s = Encoding.Unicode.GetString(val);
                    if (s.Equals("" + g) == false)
                        Assert.Fail("data does not match " + g);
                }
                else
                {
                    notfound++;
                    //Assert.Fail("item not found " + g);
                }
                c++;
                if (c % 100000 == 0)
                    Console.Write(".");
            }
            Console.WriteLine("\r\nfetch total time = " + DateTime.Now.Subtract(dt).TotalSeconds);
            if (notfound > 0)
            {
                Console.WriteLine("items not found  = " + notfound);
                Assert.Fail("items not found");
            }
            Console.WriteLine("ALL DONE OK");
            db.Shutdown();
            db = null;
        }

        [Test]
        public static void Reread_10_mill()
        {
            int count = 10 * 1000000;
            Console.WriteLine("starting...");
            var db = new RaptorDBGuid("c:\\RaptorDbTest\\" + count);
            //db.SaveIndex(true);
            var guids = new List<Guid>();
            Console.Write("reading guid list...");

            FileStream fs = new FileStream("c:\\RaptorDbTest\\guids", FileMode.Open);
            byte[] by = new byte[16];
            for (int i = 0; i < count; i++)
            {
                fs.Read(by, 0, 16);
                guids.Add(new Guid(by));
            }
            fs.Close();
            Console.WriteLine("done");
            DateTime dt = DateTime.Now;
            int c = 0;

            dt = DateTime.Now;
            int notfound = 0;
            c = 0;
            foreach (Guid g in guids)
            {
                byte[] val;
                if (db.Get(g, out val))
                {
                    string s = Encoding.Unicode.GetString(val);
                    if (s.Equals("" + g) == false)
                        Assert.Fail("data does not match " + g);
                }
                else
                {
                    notfound++;
                    //Assert.Fail("item not found " + g);
                }
                c++;
                if (c % 100000 == 0)
                    Console.Write(".");
            }
            Console.WriteLine("\r\nfetch total time = " + DateTime.Now.Subtract(dt).TotalSeconds);
            if (notfound > 0)
            {
                Console.WriteLine("items not found  = " + notfound);
                Assert.Fail("items not found");
            }
            Console.WriteLine("ALL DONE OK");
            db.Shutdown();
            db = null;

        }

        [Test]
        public static void comparetest()
        {
            DateTime dt = DateTime.Now;
            for (int i = 0; i < 1000000; i++)
            {
                Guid g = Guid.NewGuid();
                byte[] b = g.ToByteArray();
                uint u = Helper.MurMur.Hash(b);

                byte[] b2 = Helper.GetBytes((int)u, false);
                byte[] b3 = new byte[4];
                Buffer.BlockCopy(b2, 0, b3, 0, 4);

                int j = Helper.CompareSafe(b2, b3);
            }

            Console.WriteLine("time = " + DateTime.Now.Subtract(dt).TotalSeconds);
            dt = DateTime.Now;
            for (int i = 0; i < 1000000; i++)
            {
                Guid g = Guid.NewGuid();
                byte[] b = g.ToByteArray();
                uint u = Helper.MurMur.Hash(b);

                byte[] b2 = Helper.GetBytes((int)u, false);
                byte[] b3 = new byte[4];
                Buffer.BlockCopy(b2, 0, b3, 0, 4);

                int j = Helper.CompareMemCmp(b2, b3);
            }

            Console.WriteLine("time = " + DateTime.Now.Subtract(dt).TotalSeconds);
        }
    }
}