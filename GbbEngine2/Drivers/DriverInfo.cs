﻿namespace GbbEngine2.Drivers
{
    public class DriverInfo
    {
        public enum Drivers
        {
            i000_SolarmanV5 = 0,
            i999_Random = 999,
        }

        public int DriverNo { get; set; }

        public string Name { get; set; } = "";

        
        public static List<DriverInfo> OurGetDriveInfos()
        {
            List<DriverInfo> ret = new();

            ret.Add(new DriverInfo() { DriverNo = (int)Drivers.i000_SolarmanV5, Name = "SolarmanV5" });
#if DEBUG
            ret.Add(new DriverInfo() { DriverNo = (int)Drivers.i999_Random, Name = "Random" });
#endif

            return ret;
        }

    }
}
