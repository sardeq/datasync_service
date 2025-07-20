using System.ComponentModel;
using System.Configuration.Install;
using System.ServiceProcess;

namespace DataSync_Service
{
    [RunInstaller(true)]
    public partial class ProjectInstaller : Installer
    {
        public ProjectInstaller()
        {
            var processInstaller = new ServiceProcessInstaller();
            var serviceInstaller = new ServiceInstaller();

            processInstaller.Account = ServiceAccount.LocalSystem;

            serviceInstaller.ServiceName = "DataSync_Service";
            serviceInstaller.DisplayName = "Data Sync Service";
            serviceInstaller.Description = "Synchronizes data between source and destination databases.";
            serviceInstaller.StartType = ServiceStartMode.Automatic;

            Installers.Add(processInstaller);
            Installers.Add(serviceInstaller);
        }
    }
}
