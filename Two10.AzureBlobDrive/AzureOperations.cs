#region Copyright (c) 2011 Two10 degrees
//
// (C) Copyright 2011 Two10 degrees
//      All rights reserved.
//
// This software is provided "as is" without warranty of any kind,
// express or implied, including but not limited to warranties as to
// quality and fitness for a particular purpose. Active Web Solutions Ltd
// does not support the Software, nor does it warrant that the Software
// will meet your requirements or that the operation of the Software will
// be uninterrupted or error free or that any defects will be
// corrected. Nothing in this statement is intended to limit or exclude
// any liability for personal injury or death caused by the negligence of
// Active Web Solutions Ltd, its employees, contractors or agents.
//
#endregion

namespace Two10.AzureBlobDrive
{
    using System;
    using System.Collections.Concurrent;
    using System.Collections.Generic;
    using System.Diagnostics;
    using System.IO;
    using System.Linq;
    using System.Runtime.Caching;
    using System.Text.RegularExpressions;
    using System.Threading.Tasks;
    using Dokan;
    using Microsoft.WindowsAzure;
    using Microsoft.WindowsAzure.StorageClient;

    public class AzureOperations : DokanOperations
    {
        private CloudBlobClient client;
        private MemoryCache streamCache = MemoryCache.Default;
        private MemoryCache blobCache = MemoryCache.Default;
        private MemoryCache miscCache = MemoryCache.Default;
        private ConcurrentDictionary<string, BlobStream> blobsWriting = new ConcurrentDictionary<string, BlobStream>();
        private HashSet<string> virtualFolders = new HashSet<string>();
        private Dictionary<string, string> locks = new Dictionary<string, string>();

        public AzureOperations(string connectionString)
        {
            CloudStorageAccount account = CloudStorageAccount.Parse(connectionString);
            client = account.CreateCloudBlobClient();
            var container = client.GetContainerReference("$root");
            container.CreateIfNotExist();
        }

        public int Cleanup(string filename, DokanFileInfo info)
        {
            return 0;
        }

        public int CloseFile(string filename, DokanFileInfo info)
        {
            BlobStream stream = null;
            if (blobsWriting.TryRemove(filename, out stream))
            {
                Trace.WriteLine(string.Format("CloseFile {0}", filename));
                stream.Commit();
                stream.Dispose();
                this.blobCache.Remove(filename);
                stream.Blob.Container.InvalidateCache();
            }

            return 0;
        }

        public int CreateDirectory(string filename, DokanFileInfo info)
        {
            try
            {

                Trace.WriteLine(string.Format("CreateDirectory {0}", filename));

                string containerName = filename.ToContainerName(false);

                if (!IsContainerNameValid(containerName))
                {
                    return -DokanNet.ERROR_INVALID_NAME;
                }

                if (!(from c in GetAllContainers() where c.Name == containerName select c).Any())
                {
                    var container = client.GetContainerReference(containerName);
                    container.CreateIfNotExist();
                    lock (miscCache)
                    {
                        miscCache.Remove("CONTAINERS");
                    }
                }

                virtualFolders.Add(filename);

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
            Trace.WriteLine(string.Format("CreateFile {0}", filename));

            if (mode == FileMode.CreateNew)
            {
                var blob = this.GetBlob(filename, false);
                blobsWriting.TryAdd(filename, blob.OpenWrite());

                return 0;
            }

            // When trying to open a file for reading, succeed only if the file already exists.
            //if (mode == FileMode.Open && (access == FileAccess.Read || access == FileAccess.ReadWrite))
            //{
            if (GetFileInformation(filename, new FileInformation(), new DokanFileInfo(0)) == 0)
            {
                return 0;
            }
            else
            {
                return -DokanNet.ERROR_FILE_NOT_FOUND;
            }
            //}
            //else if (mode == FileMode.Create || mode == FileMode.OpenOrCreate)
            //{
            //    return 0;
            //}
            //return 0;
        }

        public int DeleteDirectory(string filename, DokanFileInfo info)
        {
            try
            {
                Trace.WriteLine(string.Format("DeleteDirectory {0}", filename));

                virtualFolders.RemoveWhere((s) => s.StartsWith(filename));

                string[] path = filename.SplitFilename();
                if (path.Length == 1)
                {
                    // a container is being deleted
                    if ((from c in GetAllContainers() where c.Name == path[0] select c).Any())
                    {
                        var container = client.GetContainerReference(path[0]);
                        container.Delete();
                        container.InvalidateCache();
                        lock (miscCache)
                        {
                            miscCache.Remove("CONTAINERS");
                        }
                    }
                    return 0;
                }

                // this is a blob
                string prefix = string.Join("/", filename.SplitFilename().Skip(1));
                var parentContainer = GetContainer(filename.ToContainerName(false));
                foreach (var blob in GetBlobsStartingWith(prefix, parentContainer, false))
                {
                    var blobDetail = GetBlobDetail(blob.Uri);
                    blobDetail.Delete();
                }
                parentContainer.InvalidateCache();
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

                var blob = this.GetBlob(filename, true);
                if (null == blob)
                {
                    return -1;
                }
                blob.Delete();
                blob.Container.InvalidateCache();
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

            var container = GetContainer(filename.ToContainerName(false));
            if (null == container)
            {
                return -1;
            }

            // write out sub folders
            foreach (var item in GetFoldersStartingWith(filename, false))
            {
                FileInformation finfo = new FileInformation();
                finfo.FileName = item;
                finfo.Attributes = System.IO.FileAttributes.Directory;
                finfo.LastAccessTime = DateTime.Now;
                finfo.LastWriteTime = DateTime.Now;
                finfo.CreationTime = DateTime.Now;
                files.Add(finfo);
            }

            // write out files
            object localsync = new object();
            string prefix = string.Join("/", filename.SplitFilename().Skip(1));

            Parallel.ForEach<IListBlobItem>(GetBlobsStartingWith(prefix, container, true), blob =>
            {
                try
                {
                    var blobDetail = GetBlobDetail(blob.Uri);
                    FileInformation finfo = new FileInformation();
                    finfo.FileName = System.Web.HttpUtility.UrlDecode(blobDetail.Uri.ToBlobFilename());
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
            return 0;

        }

        private CloudBlobContainer[] GetAllContainers()
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


        private CloudBlobContainer GetContainer(string containerName)
        {
            var container = (from c in GetAllContainers() where c.Name == containerName select c).FirstOrDefault();
            return container;
        }


        private CloudBlob GetBlob(string filename, bool mustExist)
        {
            try
            {
                if (mustExist)
                {
                    var container = GetContainer(filename.ToContainerName(true));
                    if (null == container)
                    {
                        return null;
                    }
                    string name = filename.ToBlobName();
                    var blob = (from b in container.CachedListBlobs() where System.Web.HttpUtility.UrlDecode(b.Uri.ToBlobPath()) == name select b).FirstOrDefault();
                    if (null == blob)
                    {
                        return null;
                    }
                    return container.GetBlobReference(blob.Uri.OriginalString);
                }
                else
                {
                    var container = client.GetContainerReference(filename.ToContainerName(true));
                    return container.GetBlobReference(filename.ToBlobName());
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

            if (filename == @"\")
            {
                fileinfo.Attributes = System.IO.FileAttributes.Directory;
                fileinfo.LastAccessTime = DateTime.Now;
                fileinfo.LastWriteTime = DateTime.Now;
                fileinfo.CreationTime = DateTime.Now;
                return 0;
            }

            var blob = GetBlob(filename, true);
            if (blob != null)
            {
                // this is file
                var blobDetail = GetBlobDetail(blob.Uri);
                fileinfo.FileName = System.Web.HttpUtility.UrlDecode(blobDetail.Uri.ToBlobPath());
                fileinfo.Attributes = System.IO.FileAttributes.Normal;
                fileinfo.LastAccessTime = DateTime.Now;
                fileinfo.LastWriteTime = DateTime.Now;
                fileinfo.CreationTime = DateTime.Now;
                fileinfo.Length = blobDetail.Properties.Length;
                return 0;
            }

            if (GetFoldersStartingWith(filename, true).Length > 0)
            {
                // this is a container
                fileinfo.Attributes = System.IO.FileAttributes.Directory;
                fileinfo.LastAccessTime = DateTime.Now;
                fileinfo.LastWriteTime = DateTime.Now;
                fileinfo.CreationTime = DateTime.Now;
                return 0;
            }

            if (virtualFolders.Contains(filename))
            {
                fileinfo.Attributes = System.IO.FileAttributes.Directory;
                fileinfo.LastAccessTime = DateTime.Now;
                fileinfo.LastWriteTime = DateTime.Now;
                fileinfo.CreationTime = DateTime.Now;
                return 0;
            }

            if (blobsWriting.ContainsKey(filename))
            {
                fileinfo.Attributes = System.IO.FileAttributes.Normal;
                fileinfo.LastAccessTime = DateTime.Now;
                fileinfo.LastWriteTime = DateTime.Now;
                fileinfo.CreationTime = DateTime.Now;
                fileinfo.Length = 0;
                return 0;
            }

            return -DokanNet.ERROR_FILE_NOT_FOUND;
        }

        public int LockFile(
            string filename,
            long offset,
            long length,
            DokanFileInfo info)
        {
            try
            {
                Trace.WriteLine(string.Format("LockFile {0}", filename));
                lock (this.locks)
                {
                    var blob = this.GetBlobDetail(this.GetBlob(filename, true).Uri);
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
                if (null != sourceBlob)
                {
                    var destBlob = this.GetBlob(newname, false);
                    destBlob.CopyFromBlob(sourceBlob);
                    info.IsDirectory = false;
                    destBlob.Container.InvalidateCache();
                    sourceBlob.Container.InvalidateCache();
                    destBlob.Container.InvalidateCache();
                    return 0;
                }
                else
                {
                    if (filename.SplitFilename().Length <= 1)
                    {
                        // you cannot rename a container.
                        return -1;
                    }

                    HashSet<string> newVirtualFolder = new HashSet<string>();
                    foreach (string item in this.virtualFolders)
                    {
                        newVirtualFolder.Add(item.Replace(filename, newname));
                    }
                    this.virtualFolders = newVirtualFolder;

                    return 0;

                }
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
            //const int MAX_SIZE_FOR_CACHE = 1024 * 1024;

            lock (streamCache)
            {
                MemoryStream stream = streamCache[filename] as MemoryStream;
                if (null == stream)
                {
                    var blob = GetBlob(filename, true);
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
            Trace.WriteLine(string.Format("SetFileAttributes {0}", filename));

            return -1;
        }

        public int SetFileTime(
            string filename,
            DateTime ctime,
            DateTime atime,
            DateTime mtime,
            DokanFileInfo info)
        {
            Trace.WriteLine(string.Format("SetFileTime {0}", filename));
            return -1;
        }



        public int UnlockFile(string filename, long offset, long length, DokanFileInfo info)
        {
            try
            {
                Trace.WriteLine(string.Format("UnlockFile {0}", filename));

                lock (this.locks)
                {
                    var blob = this.GetBlob(filename, true);
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
            Trace.WriteLine("Unmount");
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

            BlobStream stream = blobsWriting.GetOrAdd(filename, str => this.GetBlob(filename, false).OpenWrite());

            stream.Write(buffer, (int)0, buffer.Length);
            writtenBytes = (uint)buffer.Length;
            info.IsDirectory = false;
            return 0;
        }


        public static bool IsContainerNameValid(string containerName)
        {
            return (Regex.IsMatch(containerName, @"(^([a-z]|\d))((-([a-z]|\d)|([a-z]|\d))+)$") && (3 <= containerName.Length) && (containerName.Length <= 63));
        }

        private CloudBlob GetBlobDetail(Uri uri)
        {
            lock (blobCache)
            {
                CloudBlob blob = blobCache[uri.OriginalString] as CloudBlob;
                if (null == blob)
                {
                    blob = client.GetBlobReference(uri.OriginalString);
                    blob.FetchAttributes();
                    blobCache.Add(uri.OriginalString, blob, new CacheItemPolicy() { AbsoluteExpiration = DateTime.Now.AddMinutes(1) });
                }

                return blob;
            }

        }

        private string[] GetFoldersStartingWith(string prefix, bool matchOnly)
        {
            if ("\\" == prefix)
            {
                return (from b in GetAllContainers() where "$root" != b.Name select b.Name.SplitFilename()[0]).Distinct().ToArray();
            }

            string containerName = prefix.ToContainerName(false);

            var container = (from c in GetAllContainers() where c.Name == containerName select c).FirstOrDefault();
            if (null == container)
            {
                return new string[] { };
            }

            string blobPrefix = string.Join("/", prefix.SplitFilename().Skip(1).ToArray());

            string[] childPaths =
                (from b in container.CachedListBlobs()
                 where IsSubFolderMatch(b.Uri, blobPrefix, matchOnly)
                 select System.Web.HttpUtility.UrlDecode(b.Uri.ToBlobPathWithoutFilename()).Remove(0, blobPrefix.Length))
                 .Union(from c in virtualFolders
                        where c.StartsWith(prefix + "\\")
                        where c.Length > prefix.Length ^ matchOnly
                        select c.Remove(0, prefix.Length + 1)).Distinct().Where(s => !string.IsNullOrWhiteSpace(s)).ToArray();

            if (childPaths.Length == 0)
            {
                return childPaths;
            }
            return (from f in childPaths select f.SplitUrl()[0]).Distinct().ToArray();
        }

        private bool IsSubFolderMatch(Uri uri, string prefix, bool matchOnly)
        {
            string path = uri.ToBlobPathWithoutFilename();

            if (matchOnly)
            {
                return path.StartsWith(prefix);
            }

            if (string.IsNullOrWhiteSpace(prefix))
            {
                return !string.IsNullOrWhiteSpace(path);
            }
            return path.StartsWith(prefix) && path.Length > prefix.Length;
        }


        private IEnumerable<IListBlobItem> GetBlobsStartingWith(string prefix, CloudBlobContainer container, bool exactMatch)
        {
            return (from b in container.CachedListBlobs()
                    where BlobAtPath(System.Web.HttpUtility.UrlDecode(b.Uri.ToBlobPath()), prefix, exactMatch)
                    select b);
        }

        private bool BlobAtPath(string file, string folderPrefix, bool exactMatch)
        {
            int index = file.LastIndexOf('/');
            if (index == -1)
            {
                return string.IsNullOrWhiteSpace(folderPrefix);
            }
            if (exactMatch)
            {
                return file.Substring(0, index) == folderPrefix;    
            }
            return file.StartsWith(folderPrefix);
        }




    }

}
