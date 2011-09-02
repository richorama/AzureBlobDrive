using System;
using System.Configuration;
using System.Diagnostics;
using System.Net;
using System.Threading;
using Microsoft.WindowsAzure.ServiceRuntime;


namespace Two10.AzureBlobDrive.WorkerRole
{
    public class WorkerRole : RoleEntryPoint
    {
        public override void Run()
        {
            
            while (true)
            {
                Thread.Sleep(10000);
            }
          
        }

        public override bool OnStart()
        {
            // Set the maximum number of concurrent connections 
            ServicePointManager.DefaultConnectionLimit = 12;

            // For information on handling configuration changes
            // see the MSDN topic at http://go.microsoft.com/fwlink/?LinkId=166357.

            return base.OnStart();
        }
    }
}
