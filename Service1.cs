using System;
using System.Collections.Generic;
using System.Configuration;
using System.Data;
using System.Data.SqlClient;
using System.IO;
using System.ServiceProcess;
using System.Threading;
using System.Timers;

namespace DataSync_Service
{
    public partial class Service1 : ServiceBase
    {
        #region variables

        private System.Timers.Timer _syncTimer;
        private readonly object _syncLock = new object();
        private string _sourceConnString;
        private string _destConnString;
        private string _logFilePath;

        #endregion

        #region main

        public Service1()
        {
            InitializeComponent();
            this.ServiceName = "DataSync_Service";
            _logFilePath = Path.Combine(AppDomain.CurrentDomain.BaseDirectory, "SyncLog.txt");
        }

        protected override void OnStart(string[] args)
        {
            LogService("Service starting...");
            _sourceConnString = ConfigurationManager.ConnectionStrings["DbSource"].ConnectionString;
            _destConnString = ConfigurationManager.ConnectionStrings["DbDest"].ConnectionString;

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
                LogService("Sync started");

                SyncTable("EmpVacationsTransactions", "TransactionID");
                SyncTable("EmployeesTimeEntrance", "TimeEntranceID");
                SyncTable("EmployeesCompensation", "CompensationID");
                SyncEmployees();
                SyncTable("EmployeeAttendanceTransaction", "TransactionSerial");
                SyncTable("EmployeesPreBalances", "EmployeesPreBalancesID");

                LogService("Sync completed successfully");
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

        private void SyncTable(string tableName, string identityColumn)
        {
            try
            {
                using (var sourceConn = new SqlConnection(_sourceConnString))
                using (var destConn = new SqlConnection(_destConnString))
                {
                    sourceConn.Open();
                    destConn.Open();

                    long maxDestId = GetMaxIdentity(destConn, tableName, identityColumn);

                    DataTable newRecords = GetNewRecords(sourceConn, tableName, identityColumn, maxDestId);
                    if (newRecords.Rows.Count == 0) return;

                    LogService($"Found {newRecords.Rows.Count} new records in {tableName}");

                    ExecuteNonQuery(destConn, $"SET IDENTITY_INSERT {tableName} ON");
                    BulkInsert(destConn, tableName, newRecords);
                    ExecuteNonQuery(destConn, $"SET IDENTITY_INSERT {tableName} OFF");
                }
            }
            catch (Exception ex)
            {
                LogService($"Error syncing {tableName}: {ex.Message}");
                throw;
            }
        }

        private void SyncEmployees()
        {
            try
            {
                using (var sourceConn = new SqlConnection(_sourceConnString))
                using (var destConn = new SqlConnection(_destConnString))
                {
                    sourceConn.Open();
                    destConn.Open();

                    var existingData = GetExistingEmployees(destConn);

                    DataTable sourceEmployees = GetAllRecords(sourceConn, "Employees");
                    DataTable newEmployees = sourceEmployees.Clone();

                    foreach (DataRow row in sourceEmployees.Rows)
                    {
                        string userName = row["UserName"].ToString();
                        int employeeId = Convert.ToInt32(row["EmployeeID"]);

                        if (!existingData.UserNames.Contains(userName) &&
                            !existingData.EmployeeIds.Contains(employeeId))
                        {
                            newEmployees.ImportRow(row);
                        }
                    }

                    if (newEmployees.Rows.Count == 0) return;

                    LogService($"Found {newEmployees.Rows.Count} new records in Employees");

                    ExecuteNonQuery(destConn, "SET IDENTITY_INSERT Employees ON");
                    BulkInsert(destConn, "Employees", newEmployees);
                    ExecuteNonQuery(destConn, "SET IDENTITY_INSERT Employees OFF");
                }
            }
            catch (Exception ex)
            {
                LogService($"Error syncing Employees: {ex.Message}");
                throw;
            }
        }

        #endregion

        #region Helper Methods
        private long GetMaxIdentity(SqlConnection conn, string tableName, string columnName)
        {
            using (var cmd = new SqlCommand($"SELECT MAX({columnName}) FROM {tableName}", conn))
            {
                object result = cmd.ExecuteScalar();
                return (result == DBNull.Value) ? 0 : Convert.ToInt64(result);
            }
        }

        private DataTable GetNewRecords(SqlConnection conn, string tableName, string identityColumn, long maxId)
        {
            string query = $"SELECT * FROM {tableName} WHERE {identityColumn} > @maxId";
            using (var cmd = new SqlCommand(query, conn))
            {
                cmd.Parameters.AddWithValue("@maxId", maxId);
                using (var da = new SqlDataAdapter(cmd))
                {
                    DataTable dt = new DataTable();
                    da.Fill(dt);
                    return dt;
                }
            }
        }

        private DataTable GetAllRecords(SqlConnection conn, string tableName)
        {
            using (var cmd = new SqlCommand($"SELECT * FROM {tableName}", conn))
            using (var da = new SqlDataAdapter(cmd))
            {
                DataTable dt = new DataTable();
                da.Fill(dt);
                return dt;
            }
        }

        private (HashSet<string> UserNames, HashSet<int> EmployeeIds) GetExistingEmployees(SqlConnection conn)
        {
            var userNames = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
            var employeeIds = new HashSet<int>();

            using (var cmd = new SqlCommand("SELECT UserName, EmployeeID FROM Employees", conn))
            using (var reader = cmd.ExecuteReader())
            {
                while (reader.Read())
                {
                    userNames.Add(reader.GetString(0));
                    employeeIds.Add(reader.GetInt32(1));
                }
            }
            return (userNames, employeeIds);
        }

        private void BulkInsert(SqlConnection conn, string tableName, DataTable data)
        {
            using (var bulkCopy = new SqlBulkCopy(conn, SqlBulkCopyOptions.KeepIdentity, null))
            {
                bulkCopy.DestinationTableName = tableName;
                bulkCopy.BatchSize = 1000;
                bulkCopy.WriteToServer(data);
            }
        }

        private void ExecuteNonQuery(SqlConnection conn, string commandText)
        {
            using (var cmd = new SqlCommand(commandText, conn))
            {
                cmd.ExecuteNonQuery();
            }
        }

        private void LogService(string message)
        {
            try
            {
                File.AppendAllText(_logFilePath, $"{DateTime.Now:yyyy-MM-dd HH:mm:ss} - {message}\n");
            }
            catch { /* trash */ }
        }

        #endregion
    }
}