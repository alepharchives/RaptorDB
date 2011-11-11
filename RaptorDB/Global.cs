using System;
using System.Collections.Generic;
using System.Text;

namespace RaptorDB
{
    internal class Global
    {
        // number of items in each page
        public static short DEFAULTNODESIZE = 200; 

        public static int BUCKETCOUNT = 10007; // 2 mil
                                        //20011; // 4 mil
                                        //50021; // 10 mil
                                        //560689; // prime number ~ 112 mil items @ 200/page

        public static int MaxItemsBeforeIndexing = 20000;

        public static bool FreeMemoryOnCommit = false;

        public static int BitmapOffsetSwitchOverCount = 10;

        public static int FlushIndexerAfterThisManyMillion = 20;
    }
}
