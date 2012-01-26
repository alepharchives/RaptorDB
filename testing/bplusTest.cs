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
        public static void StringKeyTest()
        {
            var db = RaptorDB<string>.Open("c:\\raptordbtest\\strings", 255, true);
            for (int i = 0; i < 100000; i++)
            {
                db.Set("asdfasd" + i, "" + i);
            }
            db.Shutdown();
        }

        private static void test()
        {        
            int count = 10*1000000;
            string dbpath = "c:\\RaptorDbTest\\test1";
            var rap = RaptorDB<Guid>.Open(dbpath, true);

            Console.WriteLine("---------------");
            Console.WriteLine("Writing " + count.ToString("#,#"));
            List<Guid> guid = new List<Guid>();
            for (int i = 0; i < count; i++)
                guid.Add(Guid.NewGuid());

            DateTime dt = DateTime.Now;
            for (int i = 0; i < count; i++)
            {
                rap.Set(guid[i], ""+guid[i]);
                if (i % 100000 == 0)
                    Console.Write(".");
            }
            Console.WriteLine("set done");
            double st = DateTime.Now.Subtract(dt).TotalSeconds;
            
            //rap.Shutdown();
            //rap = RaptorDB<Guid>.Open(dbpath, true);
            dt = DateTime.Now;
            int failed = 0;
            for (int i = 0; i < count; i++)
            {
                if (i % 100000 == 0)
                    Console.Write(".");
                string str = "";
                if (rap.Get(guid[i], out str) == false)
                    failed++;
                else
                {
                    if (str != "" + guid[i])
                        failed++;
                }
            }
            Console.WriteLine("get done");
            if (failed>0)
                Console.WriteLine("failed count = " + failed);
            Console.WriteLine(rap.GetStatistics());
            Console.WriteLine("set time = " + st);
            Console.WriteLine("get time = " + DateTime.Now.Subtract(dt).TotalSeconds);
            rap.Shutdown();
        }

        private static void testnoget()
        {
            int count = 100 * 1000000;
            string dbpath = "c:\\RaptorDbTest\\test1";
            var rap = RaptorDB<Guid>.Open(dbpath, true);

            Console.WriteLine("---------------");
            Console.WriteLine("Writing " + count.ToString("#,#"));
            //List<Guid> guid = new List<Guid>();
            //for (int i = 0; i < count; i++)
            //    guid.Add(Guid.NewGuid());

            DateTime dt = DateTime.Now;
            for (int i = 0; i < count; i++)
            {
                Guid g = Guid.NewGuid();
                rap.Set(g, "" + g);
                if (i % 100000 == 0)
                    Console.Write(".");
            }
            Console.WriteLine("set done");
            double st = DateTime.Now.Subtract(dt).TotalSeconds;

            //rap.Shutdown();
            //rap = RaptorDB<Guid>.Open(dbpath, true);
            //dt = DateTime.Now;
            //int failed = 0;
            //for (int i = 0; i < count; i++)
            //{
            //    if (i % 100000 == 0)
            //        Console.Write(".");
            //    string str = "";
            //    if (rap.Get(guid[i], out str) == false)
            //        failed++;
            //    else
            //    {
            //        if (str != "" + guid[i])
            //            failed++;
            //    }
            //}
            //Console.WriteLine("get done");
            //if (failed > 0)
            //    Console.WriteLine("failed count = " + failed);
            Console.WriteLine(rap.GetStatistics());
            Console.WriteLine("set time = " + st);
            //Console.WriteLine("get time = " + DateTime.Now.Subtract(dt).TotalSeconds);
            rap.Shutdown();
        }

        public static void RunPageCount(ushort count)
        {
            Console.WriteLine("page item count = " + count);
            RaptorDB.Global.PageItemCount = count;
            Directory.Delete("c:\\RaptorDbTest\\", true);
            test();
        }

        public static void Main()
        {
            test();
            //RunPageCount(30000);
            //RunPageCount(20000);
            //RunPageCount(10000);
            //RunPageCount(5000);
            //RunPageCount(1000);

            Console.ReadKey();            
            return;
        }
    }
}
