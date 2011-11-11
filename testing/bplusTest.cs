using System;
using System.Diagnostics;
using System.Collections;
using System.IO;
using System.Text;
using System.Threading;
using RaptorDB;
using System.Collections.Generic;
using System.Runtime.InteropServices;

namespace testing
{
    public class bplusTest
    {
        static string s =
            // ~100 bytes
            //"{\"$type\":\"BizFX.TestApp.Entity.Customer, BizFX.TestApp.Entity, Version=1.0.0.0, Culture=neutral, PublicKeyToken=426204062733118a\"";//,\"Address\":null,\"Code\":null,\"Phone\":null,\"email\":null,\"Mobile\":null,\"ContactName\":null,\"Comments\":null,\"GUID\":\"iNIaPond7k6McmSStz14kA==\",\"BaseInfo\":{\"$type\":\"BizFX.Entity.BaseInfo, BizFX.Entity, Version=2.0.0.0, Culture=neutral, PublicKeyToken=e5d192f5e46064af\",\"RevisionNumber\":0,\"CreateDate\":\"2011-04-06 10:16:50\",\"SkipSync\":false,\"SkipDocs\":false,\"SkipRunning\":false,\"DeleteRevisions\":false,\"AssemblyFilename\":\"BizFX.TestApp.Entity.Customer, BizFX.TestApp.Entity, Version=1.0.0.0, Culture=neutral, PublicKeyToken=426204062733118a\",\"TypeName\":\"BizFX.TestApp.Entity.Customer\"},\"SecurityInfo\":{\"$type\":\"BizFX.Entity.SecurityInfo, BizFX.Entity, Version=2.0.0.0, Culture=neutral, PublicKeyToken=e5d192f5e46064af\",\"WinUserName\":\"\",\"AppUserName\":\"\",\"GUID\":\"FAfCsxxJOUuLJITZj005Ow==\",\"LoginName\":\"\",\"UserName\":\"\",\"MachineName\":\"\",\"UserDomainName\":\"\"},\"Description\":\"Base entity description.\",\"Name\":\"BaseEntity\"}";
            // ~1kb
            "{\"$type\":\"BizFX.TestApp.Entity.Customer, BizFX.TestApp.Entity, Version=1.0.0.0, Culture=neutral, PublicKeyToken=426204062733118a\",\"Address\":null,\"Code\":null,\"Phone\":null,\"email\":null,\"Mobile\":null,\"ContactName\":null,\"Comments\":null,\"GUID\":\"iNIaPond7k6McmSStz14kA==\",\"BaseInfo\":{\"$type\":\"BizFX.Entity.BaseInfo, BizFX.Entity, Version=2.0.0.0, Culture=neutral, PublicKeyToken=e5d192f5e46064af\",\"RevisionNumber\":0,\"CreateDate\":\"2011-04-06 10:16:50\",\"SkipSync\":false,\"SkipDocs\":false,\"SkipRunning\":false,\"DeleteRevisions\":false,\"AssemblyFilename\":\"BizFX.TestApp.Entity.Customer, BizFX.TestApp.Entity, Version=1.0.0.0, Culture=neutral, PublicKeyToken=426204062733118a\",\"TypeName\":\"BizFX.TestApp.Entity.Customer\"},\"SecurityInfo\":{\"$type\":\"BizFX.Entity.SecurityInfo, BizFX.Entity, Version=2.0.0.0, Culture=neutral, PublicKeyToken=e5d192f5e46064af\",\"WinUserName\":\"\",\"AppUserName\":\"\",\"GUID\":\"FAfCsxxJOUuLJITZj005Ow==\",\"LoginName\":\"\",\"UserName\":\"\",\"MachineName\":\"\",\"UserDomainName\":\"\"},\"Description\":\"Base entity description.\",\"Name\":\"BaseEntity\"}";

        public static void Twenty_Million_Optimized_GUID()
        {
            int count = 1 * 1000000;
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
            //GC.Collect(2);
            int notfound = 0;
            c = 0;
            foreach (Guid g in guids)
            {
                byte[] val;
                if (db.Get(g, out val))
                {
                    string s = Encoding.Unicode.GetString(val);
                    if (s.Equals("" + g) == false)
                        Console.WriteLine("data does not match " + g);
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
            }
            Console.WriteLine("ALL DONE OK");
            db.Shutdown();
            db = null;
        }

        static int count = 5000;
                         // 500;
        public static void Main()
        {
            Twenty_Million_Optimized_GUID();
            //Reread_20_mill();
            //Console.ReadLine();
            return;

            //TwoHundred_Million_Set_Get_BTREE();
            //return;
            //stringkeytest();
            //duplicatetest();
            //dictest();
            //ValidTest();
            //Test();
            //return;
            //purebytespeed();
            //INDEXTYPE idx = INDEXTYPE.BTREE;
            //Console.WriteLine("Index type = " + idx);
            //Console.WriteLine("Inserting " + (count * 2).ToString("#,#") + " via 2 threads");
            //DateTime dt = DateTime.Now;
            //RaptorDB.RaptorDB rap = RaptorDB.RaptorDB.Open("docs\\data.ext", 16, true, idx);
            //rap.IndexingTimerSeconds = 1000;
            //rap.InMemoryIndex = true;
            //threadtest(rap);

            //rap.IndexingTimerSeconds = 1;


            //Console.WriteLine();
            //Console.WriteLine("insert time secs = " + DateTime.Now.Subtract(dt).TotalSeconds);
            ////Console.WriteLine("press any key to stop indexing");
            ////Console.ReadKey();
            //dt = DateTime.Now;
            //rap.SaveIndex(true);
            //Console.WriteLine("save time = " + DateTime.Now.Subtract(dt).TotalSeconds);
            //long c = rap.Count();
            //Console.WriteLine("count = " + c);
            //return;
        }

        //private static void ValidTest()
        //{
        //    int count = 10000;
        //    byte[] bb = System.Text.Encoding.UTF8.GetBytes(s);
        //    List<Guid> guids = new List<Guid>();
        //    Console.WriteLine("generating guids...");
        //    for (int i = 0; i < count; i++)
        //    {
        //        guids.Add(Guid.NewGuid());
        //    }
        //    RaptorDB.RaptorDB rap = RaptorDB.RaptorDB.Open("docs\\valid.ext", 16, true, INDEXTYPE.HASH);
        //    rap.IndexingTimerSeconds = 1000;
        //    rap.InMemoryIndex = true;
        //    Console.WriteLine("saving...");
        //    for (int i = 0; i < count; i++)
        //    {
        //        rap.Set(guids[i], bb);
        //    }
        //    Console.WriteLine("checking ...");
        //    for (int i = 0; i < count; i++)
        //    {
        //        byte[] b = null;
        //        rap.Get(guids[i], out b);
        //        for (int j = 0; j < b.Length; j++)
        //        {
        //            if (b[j] != bb[j])
        //                Console.Write("x");
        //        }
        //    }
        //}

        //private static void Test()
        //{
        //    DateTime dt = DateTime.Now;
        //    Console.WriteLine("writing 1mil records");
        //    StorageFile sf = new StorageFile("pp.view", 16);
        //    sf.SkipDateTime = true;

        //    MemoryStream ms = new MemoryStream();
        //    Random r = new Random();

        //    // num cols
        //    ms.WriteByte(10);
        //    for (int i = 0; i < 10; i++)
        //    {
        //        ms.WriteByte((byte)r.Next(13));
        //        string cname = "Column" + i;
        //        ms.WriteByte((byte)cname.Length);
        //        ms.Write(Encoding.UTF8.GetBytes(cname), 0, cname.Length);
        //    }
        //    sf.WriteData(Guid.Empty.TordbByteArrayay(), ms.ToArray());

        //    for (int i = 0; i < 1000000; i++)
        //    {
        //        ms.Seek(0L, SeekOrigin.Begin);
        //        Guid g = Guid.NewGuid();

        //        ms.WriteByte(0); // deleted flag
        //        ms.WriteByte(0); // value/null flag
        //        ms.Write(BitConverter.GetBytes(r.Next()), 0, 4);
        //        ms.WriteByte(0); // value/null flag
        //        ms.Write(BitConverter.GetBytes(r.Next()), 0, 4);

        //        string s = "djfhgakjh kajhfka jhdfkghakjdfh " + i;
        //        byte[] b = Encoding.UTF8.GetBytes(s);
        //        ms.WriteByte(0); // value/null flag
        //        ms.Write(BitConverter.GetBytes((short)b.Length), 0, 2);
        //        ms.Write(Encoding.UTF8.GetBytes(s), 0, b.Length);

        //        ms.WriteByte(0); // value/null flag
        //        ms.Write(BitConverter.GetBytes(r.Next()), 0, 4);
        //        ms.WriteByte(0); // value/null flag
        //        ms.Write(BitConverter.GetBytes(r.Next()), 0, 4);
        //        ms.WriteByte(0); // value/null flag
        //        ms.Write(BitConverter.GetBytes(r.Next()), 0, 4);

        //        s = "nbnvbnmvbnmhyuh yuj fgd fg  kajhfka jhdfkghakjdfh " + i;
        //        b = Encoding.UTF8.GetBytes(s);
        //        ms.WriteByte(0); // value/null flag
        //        ms.Write(BitConverter.GetBytes((short)b.Length), 0, 2);
        //        ms.Write(Encoding.UTF8.GetBytes(s), 0, b.Length);

        //        s = "000sdf0sd0f0sd0f jhdfkghakjdfh " + i;
        //        b = Encoding.UTF8.GetBytes(s);
        //        ms.WriteByte(0); // value/null flag
        //        ms.Write(BitConverter.GetBytes((short)b.Length), 0, 2);
        //        ms.Write(Encoding.UTF8.GetBytes(s), 0, b.Length);



        //        sf.WriteData(g.TordbByteArrayay(), ms.ToArray());
        //    }
        //    sf.Shutdown();
        //    Console.WriteLine("time = " + DateTime.Now.Subtract(dt).TotalSeconds);
        //}


        //private static void NewMethod(RaptorDB.RaptorDB rap, char c)
        //{
        //    byte[] bb = System.Text.Encoding.UTF8.GetBytes(s);
        //    for (int i = 0; i < count; i++)
        //    {
        //        Guid g = Guid.NewGuid();

        //        rap.Set(g, bb);

        //        if (i % 10000 == 0)
        //        {
        //            Console.Write(c);
        //        }
        //    }
        //}

        //private static void threadtest(RaptorDB.RaptorDB bpt)
        //{
        //    Thread t1 = new Thread(delegate() { NewMethod(bpt, '.'); });
        //    Thread t2 = new Thread(delegate() { NewMethod(bpt, '-'); });

        //    t1.Start();
        //    t2.Start();
        //    t1.Join();
        //    t2.Join();
        //}
    }
}
