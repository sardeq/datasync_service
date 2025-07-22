using System;
using System.Configuration;
using System.IO;
using System.Net;
using System.ServiceProcess;
using System.Threading;
using System.Timers;

namespace DataSync_Service
{
    public partial class Service1 : ServiceBase
    {
        private System.Timers.Timer _syncTimer;
        private readonly object _syncLock = new object();
        private string _logFilePath;
        private WebServiceRef.WebService _webService = new WebServiceRef.WebService();

        public Service1()
        {
            InitializeComponent();
            ServiceName = "DataSync_Service";
            _logFilePath = Path.Combine(AppDomain.CurrentDomain.BaseDirectory, "SyncLog.txt");
            _webService.Timeout = 3600000; // 1 hour timeout
        }

        protected override void OnStart(string[] args)
        {
            ServicePointManager.ServerCertificateValidationCallback =
       ((sender, certificate, chain, sslPolicyErrors) => true);

            LogService("Service starting...");
            _syncTimer = new System.Timers.Timer(60000); // 1 minute
            _syncTimer.Elapsed += SyncDatabases;
            _syncTimer.AutoReset = true;
            _syncTimer.Start();
        }

        protected override void OnStop()
        {
            _syncTimer?.Stop();
            _syncTimer?.Dispose();
            LogService("Service stopped.");
        }

        private void SyncDatabases(object sender, ElapsedEventArgs e)
        {
            if (!Monitor.TryEnter(_syncLock)) return;

            try
            {
                _syncTimer.Stop();
                LogService("Starting sync via web service...");
                _webService.SyncAllTables();
                LogService("Sync completed via web service");
            }
            catch (Exception ex)
            {
                LogService($"Sync failed: {ex.Message}");
            }
            finally
            {
                _syncTimer.Start();
                Monitor.Exit(_syncLock);
            }
        }

        private void LogService(string message)
        {
            try
            {
                File.AppendAllText(_logFilePath, $"{DateTime.Now:yyyy-MM-dd HH:mm:ss} - {message}\n");
            }
            catch { /* Ignore log errors */ }
        }
    }
}