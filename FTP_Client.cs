using System;
using System.Xml;
using System.ServiceModel.Syndication;
using System.Net;
using System.IO;
using FluentFTP;
using System.Threading.Tasks;
using System.IO.Compression;
using System.Linq;
using System.Collections.Generic;

public class FTPClient
{
    public FTPClient()
    {

    }

    public  void getAdressInitialLoad(String url, String filepath)
    {
        String downloadLink = String.Empty;
        String[] feedwords = null;
        XmlReader reader = XmlReader.Create(url);
        SyndicationFeed feed = SyndicationFeed.Load(reader);
        reader.Close();
        foreach (SyndicationItem item in feed.Items)
        {
            String subject = item.Title.Text;
            String summary = item.Summary.Text;
            //Console.WriteLine(subject);
            Console.WriteLine(summary);
            feedwords = summary.Split(' ');
            //Console.WriteLine(words.Length);
        }
        //The link for downloading the necessary it is at the specified position
        downloadLink = feedwords[5];
        Console.WriteLine(downloadLink);

        WebClient client = new WebClient();
        client.DownloadFile(
            downloadLink, filepath);

    }

    public  async Task getFileFtp(string host, string userName, string password, string path)
    {
        FtpClient client = new FtpClient(host);

        client.Credentials = new NetworkCredential(userName, password);


        await client.ConnectAsync();


        // get a list of files and directories in the "/" folder
        foreach (FtpListItem item in await client.GetListingAsync("/"))
        {

            // if this is a file
            if (item.Type == FtpFileSystemObjectType.File)
            {

                //Check if the file already exists in the local path
                 if ( await client.CompareFileAsync(path + item.FullName, "/" + item.FullName,FtpCompareOption.Size) == FtpCompareResult.Equal)
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
        await client.DisconnectAsync();
    }

    public  void UnzipFile(String path, String extractPath)
    {
        List<String> fileEntries = Directory.GetFiles(path).ToList();
        foreach(var file in fileEntries)
        {
            if(file.Contains(".zip"))
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