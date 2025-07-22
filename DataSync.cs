using System;
using System.Collections.Generic;
using System.Configuration;
using System.Data;
using System.Data.SqlClient;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace DataSync_Service
{
    public class DataSync
    {
        private static readonly object _syncLock = new object();
        private string _destConnString;
        private string _sourceConnString;
        private string _logFilePath;

        public DataSync()
        {
            _destConnString = ConfigurationManager.ConnectionStrings["DbDest"].ConnectionString;
            _sourceConnString = ConfigurationManager.ConnectionStrings["DbSource"].ConnectionString;
            _logFilePath = Path.Combine(AppDomain.CurrentDomain.BaseDirectory, "SyncLog.txt");
        }

        public void SyncAllTables()
        {
            if (!Monitor.TryEnter(_syncLock))
            {
                LogService("Sync is already in progress. Skipping.");
                return;
            }

            try
            {
                LogService("Sync started at DataSync Class");

                using (var destConn = new SqlConnection(_destConnString))
                {
                    destConn.Open();

                    SyncTable(destConn, "EmpVacationsTransactions", "TransactionID");
                    SyncTable(destConn, "EmployeesTimeEntrance", "TimeEntranceID");
                    SyncTable(destConn, "EmployeesCompensation", "CompensationID");
                    SyncEmployees(destConn);
                    SyncTable(destConn, "EmployeeAttendanceTransaction", "TransactionSerial");
                    SyncTable(destConn, "EmployeesPreBalances", "EmployeesPreBalancesID");
                }

                LogService("Sync completed successfully at DataSync Class");
            }
            catch (Exception ex)
            {
                LogService($"Sync failed: {ex}");
            }
            finally
            {
                Monitor.Exit(_syncLock);
            }
        }

        private void SyncTable(SqlConnection destConn, string tableName, string identityColumn)
        {
            try
            {
                long maxDestId = GetMaxIdentity(destConn, tableName, identityColumn);
                DataTable newRecords = GetNewRecordsFromSource(tableName, identityColumn, maxDestId);

                if (newRecords.Rows.Count == 0) return;

                LogService($"Found {newRecords.Rows.Count} new records in {tableName}");
                ExecuteNonQuery(destConn, $"SET IDENTITY_INSERT {tableName} ON");
                BulkInsert(destConn, tableName, newRecords);
                ExecuteNonQuery(destConn, $"SET IDENTITY_INSERT {tableName} OFF");
            }
            catch (Exception ex)
            {
                LogService($"Error syncing {tableName}: {ex}");
                throw;
            }
        }

        private void SyncEmployees(SqlConnection destConn)
        {
            try
            {
                var existingUserNames = GetExistingUserNames(destConn);
                DataTable sourceEmployees = GetAllEmployeesFromSource();
                DataTable newEmployees = sourceEmployees.Clone();

                foreach (DataRow row in sourceEmployees.Rows)
                {
                    string userName = row["UserName"].ToString();
                    if (!existingUserNames.Contains(userName))
                    {
                        if (row.IsNull("AccountNo"))
                        {
                            row["AccountNo"] = "24010000";
                        }
                        string accountNo = row["AccountNo"].ToString();
                        LogService($"Inserting AccountNo: '{accountNo}' (length: {accountNo.Length})");
                        newEmployees.ImportRow(row);
                    }
                }

                if (newEmployees.Rows.Count == 0) return;

                BulkInsert(destConn, "Employees", newEmployees);
            }
            catch (Exception ex)
            {
                LogService($"Error syncing Employees: {ex.Message}");
                throw;
            }
        }

        private HashSet<string> GetExistingUserNames(SqlConnection conn)
        {
            var userNames = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
            using (var cmd = new SqlCommand("SELECT UserName FROM Employees", conn))
            using (var reader = cmd.ExecuteReader())
            {
                while (reader.Read())
                {
                    userNames.Add(reader.GetString(0));
                }
            }
            return userNames;
        }

        private DataTable GetNewRecordsFromSource(string tableName, string identityColumn, long maxId)
        {
            using (var conn = new SqlConnection(_sourceConnString))
            {
                conn.Open();
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
        }

        private DataTable GetAllEmployeesFromSource()
        {
            using (var conn = new SqlConnection(_sourceConnString))
            {
                conn.Open();
                string query = "SELECT * FROM Employees";
                using (var cmd = new SqlCommand(query, conn))
                {
                    using (var da = new SqlDataAdapter(cmd))
                    {
                        DataTable dt = new DataTable();
                        da.Fill(dt);
                        return dt;
                    }
                }
            }
        }

        private long GetMaxIdentity(SqlConnection conn, string tableName, string columnName)
        {
            using (var cmd = new SqlCommand($"SELECT MAX({columnName}) FROM {tableName}", conn))
            {
                object result = cmd.ExecuteScalar();
                return (result == DBNull.Value) ? 0 : Convert.ToInt64(result);
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
            catch { /* Ignore log errors */ }
        }
    }
}
