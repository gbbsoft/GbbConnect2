using CommunityToolkit.Mvvm.ComponentModel;
using System.Runtime.CompilerServices;
using System.Xml;

namespace GbbEngine2.Configuration
{
    public partial class SubInverter : ObservableObject
    {

        // ======================================
        // Inverter
        // ======================================

        [ObservableProperty]
        private string? m_AddressIP;

        [ObservableProperty]
        private int? m_PortNo = 8899;

        [ObservableProperty]
        private long? m_SerialNumber;

        [ObservableProperty]
        private long? m_DongleSerialNumber;


        // ======================================
        const int VERSION = 1;

        public void WriteToXML(XmlWriter xml)
        {
            xml.WriteStartElement("SubInverter");
            xml.WriteAttributeString("Version", VERSION.ToString());


            if (AddressIP!=null)
                xml.WriteAttributeString("AddressIP", AddressIP);
            if (PortNo!=null)
                xml.WriteAttributeString("PortNo", PortNo.ToString());
            if (SerialNumber!=null)
                xml.WriteAttributeString("SerialNumber", SerialNumber.ToString());
            if (DongleSerialNumber!=null)
                xml.WriteAttributeString("DongleSerialNumber", DongleSerialNumber.ToString());


            xml.WriteEndElement();
        }

        public static SubInverter ReadFromXML(XmlReader xml)
        {
            SubInverter ret = new();

            if (xml.IsStartElement("SubInverter"))
            {
                int Version = int.Parse(xml.GetAttribute("Version") ?? "");
                if (Version > VERSION)
                    throw new ApplicationException("Can't read SubInverter from newer program version!");

                string? s;
                int i;
                long l;

                ret.AddressIP = xml.GetAttribute("AddressIP");

                s = xml.GetAttribute("PortNo");
                if (s != null && int.TryParse(s, out i))
                    ret.PortNo = i;

                s = xml.GetAttribute("SerialNumber");
                if (s != null && long.TryParse(s, out l))
                    ret.SerialNumber = l;

                s = xml.GetAttribute("DongleSerialNumber");
                if (s != null && long.TryParse(s, out l))
                    ret.DongleSerialNumber = l;


                if (!xml.IsEmptyElement)
                {
                    //List<Schedule> schedules = new List<Schedule>();

                    xml.Read();
                    while (xml.NodeType != XmlNodeType.EndElement && xml.NodeType != XmlNodeType.None)
                    {
                        //if (xml.IsStartElement("SubInverter"))
                        //    ret.SubInverters.Add(SubInverter.ReadFromXML(xml));
                        //else
                            xml.Skip();

                        xml.MoveToContent();
                    }
                    xml.ReadEndElement();

                    //// sort schcedules
                    //foreach (var itm in schedules.OrderBy(q => q.Number))
                    //    ret.Schedules.Add(itm);

                }
                else
                    xml.Skip();

            }
            return ret;
        }
        // =======================================
        // Operations
        // ======================================

        public void OurCheckDataForUI()
        {
            if (SerialNumber==null)
            {
                throw new ApplicationException("SerialNumber can't be empty!");
            }

            if (DongleSerialNumber==null)
            {
                throw new ApplicationException("DongleSerialNumber can't be empty!");
            }

        }

    }
}