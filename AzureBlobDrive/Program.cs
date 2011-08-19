using System;
using System.Collections.Generic;
using System.Text;
using Microsoft.Win32;
using Dokan;

namespace Two10.AzureBlobDrive
{
    class Program
    {
        static void Main(string[] args)
        {
            DokanOptions opt = new DokanOptions();
            opt.MountPoint = "r:\\";
            opt.DebugMode = true;
            opt.UseStdErr = true;
            opt.VolumeLabel = "AZURE";
            int status = DokanNet.DokanMain(opt, new AzureOperations());
            switch (status)
            {
                case DokanNet.DOKAN_DRIVE_LETTER_ERROR:
                    Console.WriteLine("Drive letter error");
                    break;
                case DokanNet.DOKAN_DRIVER_INSTALL_ERROR:
                    Console.WriteLine("Driver install error");
                    break;
                case DokanNet.DOKAN_MOUNT_ERROR:
                    Console.WriteLine("Mount error");
                    break;
                case DokanNet.DOKAN_START_ERROR:
                    Console.WriteLine("Start error");
                    break;
                case DokanNet.DOKAN_ERROR:
                    Console.WriteLine("Unknown error");
                    break;
                case DokanNet.DOKAN_SUCCESS:
                    Console.WriteLine("Success");
                    break;
                default:
                    Console.WriteLine("Unknown status: %d", status);
                    break;

            }
        }
    }
}
