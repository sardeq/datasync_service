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

        #region Main Methods
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

        private DateTime GetLastSyncTime(string tableName)
        {
            string syncFile = Path.Combine(AppDomain.CurrentDomain.BaseDirectory, $"LastSync_{tableName}.txt");
            if (File.Exists(syncFile))
            {
                string content = File.ReadAllText(syncFile);
                if (DateTime.TryParse(content, out DateTime lastSync))
                    return lastSync;
            }
            // Use minimum SQL Server datetime value
            return new DateTime(1753, 1, 1);
        }

        private void SetLastSyncTime(string tableName, DateTime syncTime)
        {
            string syncFile = Path.Combine(AppDomain.CurrentDomain.BaseDirectory, $"LastSync_{tableName}.txt");
            File.WriteAllText(syncFile, syncTime.ToString("o"));
        }

        private void SyncTable(SqlConnection destConn, string tableName, string identityColumn)
        {
            try
            {
                DateTime lastSyncTime = GetLastSyncTime(tableName);
                DataTable changedRecords = GetChangedRecordsFromSource(tableName, lastSyncTime);

                if (changedRecords.Rows.Count == 0)
                {
                    SetLastSyncTime(tableName, DateTime.Now);
                    return;
                }

                LogService($"Found {changedRecords.Rows.Count} changed records in {tableName}");

                foreach (DataRow row in changedRecords.Rows)
                {
                    bool isDeleted = row.Table.Columns.Contains("IsDeleted") && row["IsDeleted"] != DBNull.Value && Convert.ToBoolean(row["IsDeleted"]);
                    object id = row[identityColumn];
                    if (isDeleted)
                    {
                        DeleteRecord(destConn, tableName, identityColumn, id);
                    }
                    else
                    {
                        UpsertRecord(destConn, tableName, identityColumn, row);
                    }
                }

                SetLastSyncTime(tableName, DateTime.Now);
            }
            catch (Exception ex)
            {
                LogService($"Error syncing {tableName}: {ex}");
                throw;
            }
        }

        private DataTable GetChangedRecordsFromSource(string tableName, DateTime lastSyncTime)
        {
            using (var conn = new SqlConnection(_sourceConnString))
            {
                conn.Open();
                string query = $"SELECT * FROM {tableName} WHERE LastModified > @lastSyncTime";
                using (var cmd = new SqlCommand(query, conn))
                {
                    cmd.Parameters.AddWithValue("@lastSyncTime", lastSyncTime);
                    using (var da = new SqlDataAdapter(cmd))
                    {
                        DataTable dt = new DataTable();
                        da.Fill(dt);
                        return dt;
                    }
                }
            }
        }

        private void DeleteRecord(SqlConnection conn, string tableName, string identityColumn, object id)
        {
            string query = $"DELETE FROM {tableName} WHERE {identityColumn} = @id";
            using (var cmd = new SqlCommand(query, conn))
            {
                cmd.Parameters.AddWithValue("@id", id);
                cmd.ExecuteNonQuery();
            }
        }

        private void UpsertRecord(SqlConnection conn, string tableName, string identityColumn, DataRow row)
        {
            // Build upsert logic: try update, if no rows affected, insert
            var columns = row.Table.Columns.Cast<DataColumn>().Where(c => c.ColumnName != identityColumn).ToList();
            var setClause = string.Join(", ", columns.Select(c => $"{c.ColumnName} = @{c.ColumnName}"));
            var updateQuery = $"UPDATE {tableName} SET {setClause} WHERE {identityColumn} = @{identityColumn}";
            using (var cmd = new SqlCommand(updateQuery, conn))
            {
                foreach (var col in columns)
                    cmd.Parameters.AddWithValue($"@{col.ColumnName}", row[col.ColumnName] ?? DBNull.Value);
                cmd.Parameters.AddWithValue($"@{identityColumn}", row[identityColumn]);
                int affected = cmd.ExecuteNonQuery();
                if (affected == 0)
                {
                    // Insert
                    var allColumns = row.Table.Columns.Cast<DataColumn>().ToList();
                    var colNames = string.Join(", ", allColumns.Select(c => c.ColumnName));
                    var paramNames = string.Join(", ", allColumns.Select(c => $"@{c.ColumnName}"));
                    var insertQuery = $"INSERT INTO {tableName} ({colNames}) VALUES ({paramNames})";
                    if (tableName.Equals("Employees", StringComparison.OrdinalIgnoreCase))
                    {
                        ExecuteNonQuery(conn, $"SET IDENTITY_INSERT {tableName} ON");
                    }
                    using (var insertCmd = new SqlCommand(insertQuery, conn))
                    {
                        foreach (var col in allColumns)
                            insertCmd.Parameters.AddWithValue($"@{col.ColumnName}", row[col.ColumnName] ?? DBNull.Value);
                        insertCmd.ExecuteNonQuery();
                    }
                    if (tableName.Equals("Employees", StringComparison.OrdinalIgnoreCase))
                    {
                        ExecuteNonQuery(conn, $"SET IDENTITY_INSERT {tableName} OFF");
                    }
                }
            }
        }

        private void SyncEmployees(SqlConnection destConn)
        {
            SyncTable(destConn, "Employees", "EmployeeID");
        }
        #endregion

        #region Helper Methods

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

        #endregion
    }
}
