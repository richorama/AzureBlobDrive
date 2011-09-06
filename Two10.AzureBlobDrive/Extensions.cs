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
    using System.Linq;
    using System.Runtime.Caching;
    using System.Web;
    using Microsoft.WindowsAzure.StorageClient;
    using Microsoft.WindowsAzure.StorageClient.Protocol;

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


        public static string ToContainerName(this string filename, bool includesFilename)
        {
            if (string.IsNullOrWhiteSpace(filename))
            {
                return "$root";
            }
            string[] path = filename.Split(new char[] { '\\' }, StringSplitOptions.RemoveEmptyEntries);
            if (path.Length == 0)
            {
                return "$root";
            }
            if (path.Length == 1)
            {
                if (includesFilename)
                {
                    return "$root";
                }
                else
                {
                    return path[0];
                }
            }

            return path[0];
        }




        public static string ToBlobName(this string filename)
        {
            if (string.IsNullOrWhiteSpace(filename))
            {
                return string.Empty;
            }
            string[] path = filename.Split(new char[] { '\\' }, StringSplitOptions.RemoveEmptyEntries);
            switch (path.Length)
            {
                case 0:
                    throw new ArgumentException("invalid path");
                case 1:
                    return HttpUtility.UrlEncode(path[0]);
                default:
                    return string.Join("/", (from p in path.Skip(1) select HttpUtility.UrlEncode(p)).ToArray());

            }
        }

        public static string[] SplitIntoPath(this CloudBlobContainer container)
        {
            return System.Web.HttpUtility.UrlDecode(container.Name).Split(new char[] { '/' }, StringSplitOptions.RemoveEmptyEntries);
        }

        public static string ToBlobPathWithoutFilename(this Uri uri)
        {
            var items = uri.AbsolutePath.Split(new char[] { '/' }, StringSplitOptions.RemoveEmptyEntries).Skip(1).ToArray();
            if (items.Length > 1)
            {
                return string.Join("/", items.Take(items.Length - 1));
            }
            return string.Empty;
        }

        public static string ToBlobPath(this Uri uri)
        {
            return string.Join("/", uri.AbsolutePath.Split(new char[] { '/' }, StringSplitOptions.RemoveEmptyEntries).Skip(1).ToArray());
        }

        public static string ToBlobFilename(this Uri uri)
        {
            return uri.AbsolutePath.Split(new char[] { '/' }, StringSplitOptions.RemoveEmptyEntries).Last();
        }


        private static MemoryCache blobListCache = MemoryCache.Default;
        public static IListBlobItem[] CachedListBlobs(this CloudBlobContainer container)
        {
            lock (blobListCache)
            {
                object items = blobListCache.Get(container.Name) as IListBlobItem[];
                if (null == items)
                {
                    items = container.ListBlobs(new BlobRequestOptions() { UseFlatBlobListing = true }).ToArray();
                    blobListCache.Add(container.Name, items, new CacheItemPolicy() { AbsoluteExpiration = DateTime.Now.AddMinutes(1) });
                }
                return items as IListBlobItem[];
            }
        }

        public static void InvalidateCache(this CloudBlobContainer container)
        {
            lock (blobListCache)
            {
                blobListCache.Remove(container.Name);
            }
        }

    }
}
