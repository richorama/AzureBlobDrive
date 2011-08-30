using System.Configuration;
using System.Diagnostics;
using Dokan;

namespace Two10.AzureBlobDrive.Console
{
    class Program
    {
        static void Main(string[] args)
        {
            DokanOptions opt = new DokanOptions();
            opt.MountPoint = ConfigurationManager.AppSettings["DriveLetter"];
            opt.DebugMode = true;
            opt.UseStdErr = true;
            opt.VolumeLabel = "AZURE";
            int status = DokanNet.DokanMain(opt, new AzureOperations());
            switch (status)
            {
                case DokanNet.DOKAN_DRIVE_LETTER_ERROR:
                    Trace.WriteLine("Drive letter error");
                    break;
                case DokanNet.DOKAN_DRIVER_INSTALL_ERROR:
                    Trace.WriteLine("Driver install error");
                    break;
                case DokanNet.DOKAN_MOUNT_ERROR:
                    Trace.WriteLine("Mount error");
                    break;
                case DokanNet.DOKAN_START_ERROR:
                    Trace.WriteLine("Start error");
                    break;
                case DokanNet.DOKAN_ERROR:
                    Trace.WriteLine("Unknown error");
                    break;
                case DokanNet.DOKAN_SUCCESS:
                    Trace.WriteLine("Success");
                    break;
                default:
                    Trace.WriteLine(string.Format("Unknown status: %d", status));
                    break;

            }
        }
    }
}
