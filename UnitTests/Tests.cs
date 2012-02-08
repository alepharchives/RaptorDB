using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using NUnit.Framework;
using RaptorDB;
using System.IO;
using System.Threading;

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


        [Test]
        public static void Enumerate()
        {
            var db = new RaptorDB<Guid>("c:\\RaptorDbTest\\enumerate", true);
            Guid sk = Guid.NewGuid();

            db.Set(sk, "find key");

            for (int i = 0; i < 100000; i++)
            {
                db.Set(Guid.NewGuid(), "" + i);
            }

            int found = 0;
            foreach (var enu in db.Enumerate(sk))
            {
                if (found == 0)
                {
                    string str = db.FetchRecordString(enu.Value);
                    if (str != "find key")
                    {
                        Console.WriteLine(str);
                        Assert.Fail();
                    }
                }
                found++;
            }
            Console.WriteLine("Enumerate from key count = " + found);
            db.RemoveKey(sk);
            db.Shutdown();
        }

        [Test]
        public static void Duplicates_Set_and_Get()
        {
            var db = new RaptorDB<Guid>("c:\\RaptorDbTest\\duptestfetch", true);
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
                    db.Set(g, Encoding.Unicode.GetBytes(s));
                }
            }
            db.SaveIndex();

            foreach (Guid g in guids)
            {
                int j = 0;
                foreach (int i in db.GetDuplicates(g))
                {
                    string s = db.FetchRecordString(i);
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
                //else
                //    Console.WriteLine("" + g + " = OK");
            }
            Console.WriteLine("ALL OK");
            db.Shutdown();
        }


        [Test]
        public static void One_Million_Set_Get()
        {
            Set_Get("1million", 1000000, false, false);
        }

        [Test]
        public static void One_Million_Set_Shutdown_Get()
        {
            Set_Get("1million", 1000000, true, false);
        }

        [Test]
        public static void Twenty_Million_Set_Get()
        {
            Console.WriteLine("Twenty million insert test");
            Console.WriteLine("This test will use a peak of 1.6Gb ram for guid keys");
            Console.WriteLine("This test will run for about 16 minutes depending on your hardware");
            Set_Get("20million", 20 * 1000000, false, false);
        }

        [Test]
        public static void Ten_Million_Set_Get()
        {
            Console.WriteLine("Twenty million insert test");
            Console.WriteLine("This test will use a peak of 1.2Gb ram for guid keys");
            Console.WriteLine("This test will run for about 10 minutes depending on your hardware");
            Set_Get("10million", 10 * 1000000, false, false);
        }

        [Test]
        public static void Multithread_test()
        {
            //  write this test -> 2 write threads , 1 read thread after 5 sec delay
            var db = RaptorDB.RaptorDB<Guid>.Open("c:\\RaptorDbTest\\multithread", false);

            DateTime dt = DateTime.Now;
            threadtest(db);
            Console.WriteLine("\r\ntotal time = " + DateTime.Now.Subtract(dt).TotalSeconds);
            db.Shutdown();
        }

        private static void insertthread(RaptorDB<Guid> rap, List<Guid> guids, int start, int count, char c)
        {
            for (int i = 0; i < count; i++)
            {
                rap.Set(guids[i + start], "" + (i + start));

                if (i % 100000 == 0)
                {
                    Console.Write(c);
                }
            }
        }

        private static void readthread(RaptorDB<Guid> rap, List<Guid> guids, int count, char c)
        {
            Thread.Sleep(5000);
            int notfound = 0;
            for (int i = 0; i < count; i++)
            {
                string bb = "";
                if (rap.Get(guids[i], out bb))
                {
                    if (bb != "" + i)
                        notfound++;
                }
                else
                    notfound++;
                if (i % 100000 == 0)
                {
                    Console.Write(c);
                }
            }
            if (notfound > 0)
            {
                Console.WriteLine("not found = " + notfound);
                Assert.Fail();
            }
            Console.WriteLine("read done");
        }

        private static void threadtest(RaptorDB<Guid> rap)
        {
            int count = 1000000;
            List<Guid> guids = new List<Guid>();
            Console.WriteLine("building list...");
            for (int i = 0; i < 2 * count; i++)
                guids.Add(Guid.NewGuid());
            Console.WriteLine("starting...");
            Thread t1 = new Thread(() => insertthread(rap, guids, 0, count, '.'));
            Thread t2 = new Thread(() => insertthread(rap, guids, count, count, '-'));
            Thread t3 = new Thread(() => readthread(rap, guids, count, 'R'));
            t3.Start();
            t2.Start();
            t1.Start();
            t3.Join();
            t2.Join();
            t1.Join();
        }

        private static void Set_Get(string fname, int count, bool shutdown, bool skiplist)
        {
            Console.WriteLine("One million test on ");
            var db = RaptorDB.RaptorDB<Guid>.Open("c:\\RaptorDbTest\\" + fname, false);

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
                    db.Set(g, Encoding.Unicode.GetBytes(s));
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
                    db.Set(g, Encoding.Unicode.GetBytes(s));
                    c++;
                    if (c % 10000 == 0)
                        Console.Write(".");
                    if (c % 100000 == 0)
                        Console.WriteLine("time = " + DateTime.Now.Subtract(dt).TotalSeconds);
                }
            }

            Console.WriteLine(count.ToString("#,#") + " save total time = " + DateTime.Now.Subtract(dt).TotalSeconds);
            dt = DateTime.Now;
            if (shutdown == true)
            {
                db.Dispose();
                db = null;
                db = RaptorDB.RaptorDB<Guid>.Open("c:\\RaptorDbTest\\" + fname, false);
            }
            GC.Collect(2);
            int notfound = 0;
            c = 0;
            if (skiplist == false)
            {
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

        [Test]
        public static void StringKeyTest()
        {
            var db = RaptorDB<string>.Open("c:\\raptordbtest\\strings", 255, true);
            for (int i = 0; i < 100000; i++)
            {
                db.Set("asdfasd" + i, ""+i);
            }
            db.Shutdown();
        }

        [Test]
        public static void RaptorDBString_test()
        {
            Console.WriteLine("unlimited key size test");
            var rap = new RaptorDBString(@"c:\raptordbtest\longstringkey", false);
            Console.WriteLine("inserting 100000 ...");
            for (int i = 0; i < 100000; i++)
            {
                rap.Set(string1kb + i, i.ToString());
            }

            Console.WriteLine("fetching values...");
            int notfound = 0;
            for (int i = 0; i < 100000; i++)
            {
                string str = "";
                if (rap.Get(string1kb + i, out str))
                {
                    if (i.ToString() != str)
                        Assert.Fail("value does not match");
                }
                else
                    notfound++;// Assert.Fail("value not found");
            }
            if (notfound > 0)
            {
                Console.WriteLine("values not found = " + notfound);
                Assert.Fail("values not found = " + notfound);
            }
            else
                Console.WriteLine("ALL OK");
            rap.Shutdown();
        }


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
    }
}