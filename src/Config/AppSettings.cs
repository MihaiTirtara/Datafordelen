namespace Datafordelen.Config
{
    public class AppSettings
    {
        public string InitialAddressDataUrl { get; set; }
        public string InitialAddressDataZipFilePath { get; set; }
        public string InitialAddressDataUnzipPath { get; set; }
        public string InitialAddressDataProcessedPath { get; set; }
        public int MinX { get; set; }
        public int MaxX { get; set; }
        public int MinY { get; set; }
        public int MaxY { get; set; }

        public string ftpServer {get;set;}

        public string adressUsername {get;set;}

        public string adressPassword {get;set;}
        
    }
}
