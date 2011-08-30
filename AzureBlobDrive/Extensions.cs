using System;
using Microsoft.WindowsAzure.StorageClient;
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
}
