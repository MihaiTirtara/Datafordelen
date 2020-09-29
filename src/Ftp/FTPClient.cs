using System;
using System.Xml;
using System.ServiceModel.Syndication;
using System.Net;
using System.IO;
using FluentFTP;
using System.Threading.Tasks;
using System.IO.Compression;
using System.Linq;

namespace Datafordelen.Ftp
{
    public class FTPClient
    {
        public void GetAddressInitialLoad(String url, String filepath)
        {
            var downloadLink = String.Empty;
            String[] feedwords = null;

            using var reader = XmlReader.Create(url);

            var feed = SyndicationFeed.Load(reader);
            reader.Close();
            foreach (var item in feed.Items)
            {
                var subject = item.Title.Text;
                var summary = item.Summary.Text;
                Console.WriteLine(summary);
                feedwords = summary.Split(' ');
            }

            // The link for downloading the necessary it is at the specified position
            downloadLink = feedwords[5];
            Console.WriteLine(downloadLink);

            WebClient client = new WebClient();
            client.DownloadFile(
                downloadLink, filepath);
        }

        public async Task GetFileFtp(string host, string userName, string password, string path)
        {
            using var client = new FtpClient(host);

            client.Credentials = new NetworkCredential(userName, password);

            await client.ConnectAsync();

            // get a list of files and directories in the "/" folder
            foreach (var item in await client.GetListingAsync("/"))
            {
                // if this is a file
                if (item.Type == FtpFileSystemObjectType.File)
                {

                    //Check if the file already exists in the local path
                    if (await client.CompareFileAsync(path + item.FullName, "/" + item.FullName, FtpCompareOption.Size) == FtpCompareResult.Equal)
                    {
                        Console.WriteLine("item already exists " + item.FullName);
                    }
                    else
                    {
                        Console.WriteLine("Item needs to be added " + item.FullName);
                        await client.DownloadFileAsync(path + item.FullName, "/" + item.FullName);
                    }
                }
            }

            // TODO Might not be needed
            await client.DisconnectAsync();
        }

        public void UnzipFile(string path, string extractPath)
        {
            var fileEntries = Directory.GetFiles(path).ToList();
            foreach (var file in fileEntries)
            {
                if (file.Contains(".zip"))
                {
                    ZipFile.ExtractToDirectory(file, extractPath);
                    File.Delete(file);
                }
                else
                {
                    Console.WriteLine("File already unzipped");
                }
            }
        }
    }
}
