using System;
using System.Collections.Generic;
using System.Text;

namespace RaptorDB
{
    internal enum BMP_TYPE
    {
        INDEX_LIST,
        WAH_BMP
    }

    internal class BitmapCache
    {
        public BitmapCache()
        {
            BMPType = BMP_TYPE.INDEX_LIST;
            records = new List<uint>();
        }

        public BitmapCache(BMP_TYPE type, uint[] vals)
        {
            BMPType = type;
            if (type == BMP_TYPE.INDEX_LIST)
                records = new List<uint>(vals);
            else
                bitarray = new WAHBitArray(WAH_INPUT_TYPE.Compressed_WAH, vals);
        }

        public BMP_TYPE BMPType { get; set; } 
        public WAHBitArray bitarray { get; set; }
        public List<uint> records { get; set; }
        public bool isDirty { get; set; }

        public WAHBitArray GetBitarray()
        {
            if (BMPType == BMP_TYPE.WAH_BMP)
                return bitarray;
            else
            {
                WAHBitArray b = new WAHBitArray();
                foreach (uint i in records)
                {
                    b.Set((int)i, true);
                }
                return b;
            }
        }

        public uint[] GetBits()
        {
            if (BMPType == BMP_TYPE.INDEX_LIST)
                return records.ToArray();
            else
                return bitarray.GetCompressed();
        }
    }
}
