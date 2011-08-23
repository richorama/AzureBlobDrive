using System;
using System.Linq;
using System.Collections.Generic;
using System.Text;
using Microsoft.Win32;
using Dokan;
using Microsoft.WindowsAzure;
using Microsoft.WindowsAzure.StorageClient;
using System.IO;
using System.Text.RegularExpressions;
using System.Runtime.Caching;
using System.Configuration;
using System.Diagnostics;
using System.Threading.Tasks;
using Microsoft.WindowsAzure.StorageClient.Protocol;

namespace Two10.AzureBlobDrive
{

    static class Extensions
    {
        public static string AcquireLease(this CloudBlob blob)
        {
            var creds = blob.ServiceClient.Credentials;
            var transformedUri = new Uri(creds.TransformUri(blob.Uri.ToString()));
            var req = BlobRequest.Lease(transformedUri, 60, LeaseAction.Acquire, null);
            blob.ServiceClient.Credentials.SignRequest(req);
            using (var response = req.GetResponse())
            {
                return response.Headers["x-ms-lease-id"];
            }
        }

        public static void ReleaseLease(this CloudBlob blob, string leaseId)
        {
            var creds = blob.ServiceClient.Credentials;
            var transformedUri = new Uri(creds.TransformUri(blob.Uri.ToString()));
            var req = BlobRequest.Lease(transformedUri, 0, LeaseAction.Release, leaseId);
            blob.ServiceClient.Credentials.SignRequest(req);
            using (var response = req.GetResponse())
            {
            }
        }
    }


    class AzureOperations : DokanOperations
    {
        private CloudBlobClient client;
        private MemoryCache streamCache = MemoryCache.Default;
        private MemoryCache blobCache = MemoryCache.Default;
        private MemoryCache miscCache = MemoryCache.Default;
        private Dictionary<string, string > locks = new Dictionary<string, string>();

        public AzureOperations()
        {
            CloudStorageAccount account = CloudStorageAccount.Parse(ConfigurationManager.AppSettings["AzureConnectionString"]);
            client = account.CreateCloudBlobClient();
        }

        public int Cleanup(string filename, DokanFileInfo info)
        {
            return 0;
        }

        public int CloseFile(string filename, DokanFileInfo info)
        {
            if (info.Context != null)
            {
                var dictionary = info.Context as Dictionary<string, BlobStream>;
                if (dictionary.ContainsKey(filename))
                {
                    Trace.WriteLine(string.Format("CloseFile {0}", filename));
                    BlobStream stream = dictionary[filename];
                    stream.Commit();
                    stream.Dispose();
                    this.blobCache.Remove(filename);
                }
                dictionary.Remove(filename);
            }

            return 0;
        }

        public int CreateDirectory(string filename, DokanFileInfo info)
        {
            try
            {

                Trace.WriteLine(string.Format("CreateDirectory {0}", filename));

                string[] path = filename.Split('\\');
                if (path.Length > 2)
                {
                    return -1;
                }
                string newContainerName = path.Last();
                
                if (!IsContainerNameValid(newContainerName))
                {
                    return -1;
                }
                var container = client.GetContainerReference(newContainerName);
                container.CreateIfNotExist();
                lock (miscCache)
                {
                    miscCache.Remove("CONTAINERS");
                }
                return 0;
            }
            catch (Exception ex)
            {
                Trace.WriteLine(ex.ToString());
                return -1;
            }
        }

        public int CreateFile(
            string filename,
            System.IO.FileAccess access,
            System.IO.FileShare share,
            System.IO.FileMode mode,
            System.IO.FileOptions options,
            DokanFileInfo info)
        {
            return 0;            
        }

        public int DeleteDirectory(string filename, DokanFileInfo info)
        {
            try
            {
                Trace.WriteLine(string.Format("DeleteDirectory {0}", filename));

                var container = this.GetContainer(filename);
                if (null == container)
                {
                    return -1;
                }
                container.Delete();
                lock (miscCache)
                {
                    miscCache.Remove("CONTAINERS");
                }
                return 0;
            }
            catch (Exception ex)
            {
                Trace.WriteLine(ex.ToString());
                return -1;
            }
        }

        public int DeleteFile(string filename, DokanFileInfo info)
        {
            try
            {
                Trace.WriteLine(string.Format("DeleteFile {0}", filename));

                var blob = this.GetBlob(filename);
                if (null == blob)
                {
                    return -1;
                }
                blob.Delete();
                return 0;
            }
            catch (Exception ex)
            {
                Trace.WriteLine(ex.ToString());
                return -1;
            }
        }


        public int FlushFileBuffers(
            string filename,
            DokanFileInfo info)
        {
            return -1;
        }

        public int FindFiles(
            string filename,
            System.Collections.ArrayList files,
            DokanFileInfo info)
        {
            Trace.WriteLine(string.Format("FindFiles {0}", filename));

            if (filename == "\\")
            {
                foreach (var container in GetContainers())
                {
                    FileInformation finfo = new FileInformation();
                    finfo.FileName = container.Name;
                    finfo.Attributes = System.IO.FileAttributes.Directory;
                    finfo.LastAccessTime = DateTime.Now;
                    finfo.LastWriteTime = DateTime.Now;
                    finfo.CreationTime = DateTime.Now;
                    files.Add(finfo);
                }
                return 0;
            }
            else
            {
                var container = GetContainer(filename);
                if (null == container)
                {
                    return -1;
                }
                object localsync = new object();
        
                Parallel.ForEach<IListBlobItem>(container.ListBlobs(), blob =>
                {
                    try
                    {
                        var blobDetail = GetBlobDetail(blob.Uri.OriginalString);
                        FileInformation finfo = new FileInformation();
                        finfo.FileName =  System.Web.HttpUtility.UrlDecode(blobDetail.Uri.Segments.Last());
                        finfo.Attributes = System.IO.FileAttributes.Normal;
                        finfo.LastAccessTime = DateTime.Now;
                        finfo.LastWriteTime = DateTime.Now;
                        finfo.CreationTime = DateTime.Now;
                        finfo.Length = blobDetail.Properties.Length;
                        lock (localsync)
                        {
                            files.Add(finfo);
                        }
                    }
                    catch { }
                });

                // TODO: Work out the sub containers here
                return 0;

                
            }
        }

        private CloudBlobContainer[] GetContainers()
        {
            lock (miscCache)
            {
                var containers = miscCache.Get("CONTAINERS") as CloudBlobContainer[];
                if (null == containers)
                {
                    containers = this.client.ListContainers().ToArray();
                    miscCache.Add("CONTAINERS", containers, new CacheItemPolicy() { AbsoluteExpiration = DateTime.Now.AddMinutes(1) });
                }
                return containers;    
            }
        }


        private CloudBlobContainer GetContainer(string filename)
        {
            var path = filename.Split('\\');
            var container = (from c in GetContainers() where c.Name == path[1] select c).FirstOrDefault();
            return container;
        }

        private CloudBlob GetBlob(string filename)
        {
            return this.GetBlob(filename, true);
        }
        
        private CloudBlob GetBlob(string filename, bool mustExit)
        {
            try
            {
                if (mustExit)
                {
                    var container = GetContainer(filename);
                    if (null == container)
                    {
                        return null;
                    }
                    var blobs = container.ListBlobs();
                    string[] path = filename.Split('\\');
                    string name =  path.Last();
                    var blob = (from b in blobs where System.Web.HttpUtility.UrlDecode(b.Uri.Segments.Last()) == name select b).FirstOrDefault();
                    if (null == blob)
                    {
                        return null;
                    }
                    var blobDetail = container.GetBlobReference(blob.Uri.OriginalString);
                    return blobDetail;
                }
                else
                {
                    var path = filename.Split('\\').Skip(1).ToArray();
                    var file = path.Last();
                    path = path.Take(path.Length - 1).ToArray();
                    var container = client.GetContainerReference(string.Join("/", path));
                    return container.GetBlobReference(file);
                }
            }
            catch (Exception ex)
            {
                Trace.WriteLine(ex.ToString());
                return null;
            }
        }


        public int GetFileInformation(
            string filename,
            FileInformation fileinfo,
            DokanFileInfo info)
        {
            Trace.WriteLine(string.Format("GetFileInformation {0}", filename));

            if (filename == "\\")
            {
                fileinfo.Attributes = System.IO.FileAttributes.Directory;
                fileinfo.LastAccessTime = DateTime.Now;
                fileinfo.LastWriteTime = DateTime.Now;
                fileinfo.CreationTime = DateTime.Now;

                return 0;
            }

            var container = GetContainer(filename);
            if (container == null)
                return -1;

            var blob = GetBlob(filename, true);
            
            if (blob != null)
            {
                var blobDetail = GetBlobDetail(blob.Uri.OriginalString);
                fileinfo.FileName = System.Web.HttpUtility.UrlDecode(blobDetail.Uri.Segments.Last());
                fileinfo.Attributes = System.IO.FileAttributes.Normal;
                fileinfo.LastAccessTime = DateTime.Now;
                fileinfo.LastWriteTime = DateTime.Now;
                fileinfo.CreationTime = DateTime.Now;
                fileinfo.Length = blobDetail.Properties.Length;
            }
            else
            {
                fileinfo.Attributes = System.IO.FileAttributes.Directory;
                fileinfo.LastAccessTime = DateTime.Now;
                fileinfo.LastWriteTime = DateTime.Now;
                fileinfo.CreationTime = DateTime.Now;
            }
            return 0;
        }

        public int LockFile(
            string filename,
            long offset,
            long length,
            DokanFileInfo info)
        {
            try
            {
                lock (this.locks)
                {
                    var blob = this.GetBlobDetail(filename);
                    this.locks.Add(filename, blob.AcquireLease());
                    return 0;
                }
            }
            catch (Exception ex)
            {
                Trace.WriteLine(ex.ToString());
                return -1;
            }
        }

        public int MoveFile(
            string filename,
            string newname,
            bool replace,
            DokanFileInfo info)
        {
            Trace.WriteLine(string.Format("MoveFile {0}", filename));

            try
            {
                var sourceBlob = this.GetBlob(filename, true);
                var destBlob = this.GetBlob(newname, false);
                destBlob.CopyFromBlob(sourceBlob);
                info.IsDirectory = false;
                return 0;
            }
            catch (Exception ex)
            {
                Trace.WriteLine(ex.ToString());
                return -1;
            }

        }

        public int OpenDirectory(string filename, DokanFileInfo info)
        {
            return 0;
        }



        private MemoryStream GetStream(string filename)
        {
            const int MAX_SIZE_FOR_CACHE = 1024 * 1024;

            lock (streamCache)
            {
                MemoryStream stream = streamCache[filename] as MemoryStream;
                if (null == stream)
                {
                    var blob = GetBlob(filename);
                    if (null == blob)
                    {
                        return null;
                    }
                    stream = new MemoryStream();
                    blob.DownloadToStream(stream);
                    //if (stream.Length < MAX_SIZE_FOR_CACHE)
                    //{
                        // don't cache huge files
                        streamCache.Add(filename, stream, new CacheItemPolicy() { AbsoluteExpiration = DateTime.Now.AddMinutes(1) });
                    //}
                }
                stream.Position = 0;
                return stream;
            }
        }


        public int ReadFile(
            string filename,
            byte[] buffer,
            ref uint readBytes,
            long offset,
            DokanFileInfo info)
        {
            Trace.WriteLine(string.Format("ReadFile {0}", filename));

            try
            {
                MemoryStream ms = GetStream(filename);
                if (null == ms)
                {
                    return -1;
                }
                ms.Seek(offset, SeekOrigin.Begin);
                readBytes = (uint)ms.Read(buffer, 0, buffer.Length);
                return 0;
            }
            catch (Exception ex)
            {
                Trace.WriteLine(ex);
                return -1;
            }
        }

        public int SetEndOfFile(string filename, long length, DokanFileInfo info)
        {
            return 0;
        }

        public int SetAllocationSize(string filename, long length, DokanFileInfo info)
        {
            return -1;
        }

        public int SetFileAttributes(
            string filename,
            System.IO.FileAttributes attr,
            DokanFileInfo info)
        {
            return -1;
        }

        public int SetFileTime(
            string filename,
            DateTime ctime,
            DateTime atime,
            DateTime mtime,
            DokanFileInfo info)
        {
            return -1;
        }

       

        public int UnlockFile(string filename, long offset, long length, DokanFileInfo info)
        {
            try
            {
                lock (this.locks)
                {
                    var blob = this.GetBlob(filename);
                    if (this.locks.ContainsKey(filename))
                    {
                        blob.ReleaseLease(this.locks[filename]);
                    }
                    return 0;
                }
            }
            catch (Exception ex)
            {
                Trace.WriteLine(ex.ToString());
                return -1;
            }
        }

        public int Unmount(DokanFileInfo info)
        {
            return 0;
        }

        public int GetDiskFreeSpace(
           ref ulong freeBytesAvailable,
           ref ulong totalBytes,
           ref ulong totalFreeBytes,
           DokanFileInfo info)
        {
            freeBytesAvailable = 512 * 1024 * 1024;
            totalBytes = 1024 * 1024 * 1024;
            totalFreeBytes = 512 * 1024 * 1024;
            return 0;
        }

        public int WriteFile(
            string filename,
            byte[] buffer,
            ref uint writtenBytes,
            long offset,
            DokanFileInfo info)
        {
            Trace.WriteLine(string.Format("WriteFile {0}", filename));

            if (info.Context == null)
            {
                info.Context = new Dictionary<string, BlobStream>();
            }
            Dictionary<string, BlobStream> dictionary = info.Context as Dictionary<string, BlobStream>;
            if (!dictionary.ContainsKey(filename))
            {
                var blob = this.GetBlob(filename, false);
                dictionary.Add(filename, blob.OpenWrite());
            }

            dictionary[filename].Write(buffer, (int) 0, buffer.Length);
            writtenBytes = (uint) buffer.Length;
            info.IsDirectory = false;
            return 0;
        }

       
        public static bool IsContainerNameValid(string containerName)
        {
            return (Regex.IsMatch(containerName, @"(^([a-z]|\d))((-([a-z]|\d)|([a-z]|\d))+)$") && (3 <= containerName.Length) && (containerName.Length <= 63));
        }

        private CloudBlob GetBlobDetail(string uri)
        {
            lock (blobCache)
            {
                CloudBlob blob = blobCache[uri] as CloudBlob;
                if (null == blob)
                {
                    blob = client.GetBlobReference(uri);
                    blob.FetchAttributes();
                    blobCache.Add(uri, blob, new CacheItemPolicy() { AbsoluteExpiration = DateTime.Now.AddMinutes(1) });
                }

                return blob;
            }
        
        }




    }

}
