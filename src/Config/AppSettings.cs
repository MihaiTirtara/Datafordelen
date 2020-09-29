using System.Collections.Generic;


namespace Datafordelen.Config
{
    public class AppSettings
    {
        public string InitialAddressDataUrl { get; set; }
        public string InitialAddressDataZipFilePath { get; set; }
        public string InitialAddressDataUnzipPath { get; set; }
        public string InitialAddressDataProcessedPath { get; set; }
        public double MinX { get; set; }
        public double MaxX { get; set; }
        public double MinY { get; set; }
        public double MaxY { get; set; }

        public string ftpServer { get; set; }

        public string adressUserName { get; set; }

        public string adressPassword { get; set; }

        public string geoUserName { get; set; }

        public string geoPassword { get; set; }

        public string geoUnzipPath { get; set; }

        public string geoGmlPath { get; set; }

        public string geoProcessedPath { get; set; }

        public List<string> geoFieldList {get;set;}
    }
}
