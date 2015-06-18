using System;
using System.Collections;
using System.Collections.Generic;
using System.Data;
using MongoDB.Driver;
using USC.GISResearchLab.Common.Core.Databases;
using USC.GISResearchLab.Common.Core.Utils.Arrays;
using USC.GISResearchLab.Common.Databases.QueryManagers;
using USC.GISResearchLab.Common.Databases.SchemaManagers;
using USC.GISResearchLab.Common.Databases.SqlServer;
using USC.GISResearchLab.Common.Databases.TypeConverters;
using USC.GISResearchLab.Common.Utils.Databases.TableDefinitions;

namespace TAMU.GeoInnovation.Common.Utils.Databases.MongoDB
{
    public class MongoDBSchemaManager : AbstractSchemaManager
    {
        public MongoDBSchemaManager()
        {
            DataProviderType = DataProviderType.MongoDB;
            DatabaseType = DatabaseType.MongoDB;
            QueryManager = new QueryManager(DataProviderType, DatabaseType);
        }

        public MongoDBSchemaManager(string connectionString)
        {
            
            DataProviderType = DataProviderType.MongoDB;
            DatabaseType = DatabaseType.MongoDB;
            ConnectionString = connectionString;
            QueryManager = new QueryManager(DataProviderType, DatabaseType, ConnectionString);

        }

        public MongoDBSchemaManager(string pathToDatabaseDlls, string connectionString)
        {

            DataProviderType = DataProviderType.MongoDB;
            DatabaseType = DatabaseType.MongoDB;
            ConnectionString = connectionString;
            QueryManager = new QueryManager(pathToDatabaseDlls, DataProviderType, DatabaseType, ConnectionString);

        }
       
        public override void CreateDatabase()
        {
            throw new NotImplementedException();
        }

        public override TableDefinition GetTableDefinition(string table)
        {
            TableDefinition ret = null;
            try
            {
                TableColumn[] tableColumns = GetColumns(table);
                if (tableColumns != null)
                {
                    if (ret == null)
                    {
                        ret = new TableDefinition(DataProviderType, table);
                    }
                    ret.TableColumns = tableColumns;
                }
            }
            catch (Exception ex)
            {

                string msg = "Error getting table definition: " + ex.Message;
                throw new Exception(msg, ex);
            }
            return ret;
        }

        public override void AddTableToDatabase(string tableName, string sql)
        {
            try
            {
                MongoDBConnection mongoDBConnection = (MongoDBConnection)QueryManager.Connection;
                if (mongoDBConnection != null)
                {
                    MongoServer mongoServer = mongoDBConnection.MongoServer;

                    if (mongoServer != null)
                    {

                        MongoDatabase database = mongoServer.GetDatabase(DefaultDatabase);
                        database.CreateCollection(tableName);

                    }
                }
            }
            catch (Exception ex)
            {
                string msg = "Error removing table: " + ex.Message;
                throw new Exception(msg, ex);
            }
        }

        public override void AddColumnsToTable(string tableName, string[] columnNames, DatabaseSuperDataType[] dataTypes)
        {
            for (int i = 0; i < columnNames.Length; i++)
            {
                AddColumnToTable(tableName, columnNames[i], dataTypes[i]);
            }
        }

        public override void AddColumnToTable(string tableName, string columnName, DatabaseSuperDataType dataType)
        {
            AddColumnToTable(tableName, columnName, dataType, true, 0, 0);
        }

        public override void AddColumnToTable(string tableName, string columnName, DatabaseSuperDataType dataType, bool nullable, int maxLength, int precision)
        {
            throw new NotImplementedException();
        }

        public override string BuildCreateTableStatement(TableDefinition tableDefinition)
        {
            throw new NotImplementedException("There is no table in MongoDB");
        }

        public static string BuildConnectionString(string dataSource, string catalog, string userName, string password)
        {
            MongoConnectionStringBuilder builder = new MongoConnectionStringBuilder();
            builder.DatabaseName = catalog;
            builder.Server = new MongoServerAddress(dataSource);
            builder.Username = userName;
            builder.Password = password;
            return builder.ToServerSettings().ToString();
        }

        public static bool DatabaseIsValid(string dataSource, string catalog, string userName, string password)
        {
            bool ret = false;
            MongoServer server = null;
            try
            {
                MongoConnectionStringBuilder builder = new MongoConnectionStringBuilder();
                builder.DatabaseName = catalog;
                builder.Server = new MongoServerAddress(dataSource);
                builder.Username = userName;
                builder.Password = password;
                server = MongoServer.Create(builder);
                server.Connect();
                ret = true;
            }
            catch (Exception)
            {
                if (server != null)
                {
                    if (server.State == MongoServerState.Connected)
                    {
                        server.Disconnect();
                    }

                }
            }
            finally
            {
                if (server != null)
                {
                    if (server.State == MongoServerState.Connected)
                    {
                        server.Disconnect();
                    }

                }
            }
            return ret;
        }

        //public static MongoServer GetConnection(string connectionString)
        //{
        //    MongoServer mongoServer = null;
        //    try
        //    {
        //       mongoServer = MongoServer.Create(connectionString);
        //    }
        //    catch (Exception ex)
        //    {
        //        if (mongoServer != null)
        //        {
        //            if (mongoServer.State == MongoServerState.Connected)
        //            {
        //                mongoServer.Disconnect();
        //            }
        //        }
        //        string msg = "Error getting MongoDB database connection: " + ex.Message;
        //        throw new Exception(msg, ex);
        //    }
        //    return mongoServer;
        //}

        //public static MongoServer GetConnection(string dataSource, string catalog, string userName, string password)
        //{
          
        //    MongoServer mongoServer = null;
        //    try
        //    {
        //        MongoConnectionStringBuilder builder = new MongoConnectionStringBuilder();
        //        builder.DatabaseName = catalog;
        //        builder.Server = new MongoServerAddress(dataSource);
        //        builder.Username = userName;
        //        builder.Password = password;
        //        mongoServer = MongoServer.Create(builder);

        //    }
        //    catch (Exception ex)
        //    {
        //        if (mongoServer != null)
        //        {
        //            if (mongoServer.State == MongoServerState.Connected)
        //            {
        //                mongoServer.Disconnect();
        //            }
        //        }
        //        string msg = "Error getting MongoDB database connection: " + ex.Message;
        //        throw new Exception(msg, ex);
        //    }
        //    return mongoServer;
        //}


        public ArrayList GetTablesAsArrayList()
        {
            ArrayList ret = null;

            try
            {
                DataTable schemaTable = GetTablesAsDataTable();
                if (schemaTable != null)
                {
                    ret = new ArrayList();
                    for (int i = 0; i < schemaTable.Rows.Count; i++)
                    {
                        ret.Add(schemaTable.Rows[i].ItemArray[0].ToString());
                    }
                }
            }
            catch (Exception ex)
            {
                string msg = "Error getting table names: " + ex.Message;
                throw new Exception(msg, ex);
            }
            return ret;
        }

        public override TableColumn[] GetColumns(string tableName)
        {
            return GetColumns(tableName, true);
        }


        public override TableColumn[] GetColumns(string tableName, bool shouldOpenAndClose)
        {
            throw new NotImplementedException("MongoDB does not have fixed columns");
        }

        public string GetCreateTableStatement(string tableName)
        {
                throw new NotImplementedException("MongoDB does not have columns");
        }

        public override string[] GetDatabases()
        {
            return GetDatabases(false, DatabaseNameListingOptions.AllDatabases);
        }

        public override string[] GetDatabases(bool shouldOpenAndClose)
        {
            return GetDatabases(shouldOpenAndClose, DatabaseNameListingOptions.AllDatabases);
        }

        public override string[] GetDatabases(DatabaseNameListingOptions opt)
        {
            return GetDatabases(false, opt);
        }

        public override string[] GetDatabases(bool shouldOpenAndClose, DatabaseNameListingOptions opt)
        {
            string[] ret = null;
            DataTable dataTable = GetDatabasesAsDataTable();

            if (dataTable != null && dataTable.Rows.Count > 1)
            {
                List<string> names = new List<string>();

                foreach (DataRow dataRow in dataTable.Rows)
                {
                    names.Add(Convert.ToString(dataRow[0]));
                }

                ret = names.ToArray();
            }

            return ret;
        }

        public override DataTable GetDatabasesAsDataTable()
        {
            return GetDatabasesAsDataTable(false, DatabaseNameListingOptions.AllDatabases);
        }

        public override DataTable GetDatabasesAsDataTable(bool shouldOpenAndClose)
        {
            return GetDatabasesAsDataTable(shouldOpenAndClose, DatabaseNameListingOptions.AllDatabases);
        }

        public override DataTable GetDatabasesAsDataTable(bool shouldOpenAndClose, DatabaseNameListingOptions opt)
        {

            DataTable ret = null;

          
            try
            {

               MongoDBConnection mongoDBConnection  = (MongoDBConnection)QueryManager.Connection;
               if (mongoDBConnection != null)
               {
                   MongoServer mongoServer = mongoDBConnection.MongoServer;

                   if (mongoServer != null)
                   {


                       List<String> dbNames = (List<String>)mongoServer.GetDatabaseNames();
                       ret = new DataTable();
                       ret.Columns.Add(new DataColumn("name"));

                       foreach (string name in dbNames)
                       {
                           DataRow dataRow = ret.NewRow();
                           dataRow["name"] = name;
                           ret.Rows.Add(dataRow);
                       }
                   }
               }

            }
            catch (Exception ex)
            {
                string msg = "Error getting GetDatabasesAsDataTable: " + ex.Message;
                throw new Exception(msg, ex);
            }
            return ret;

        }

        public override string[] GetTables()
        {
            return GetTables(true);
        }

        public override string[] GetTables(bool shouldOpenAndClose)
        {
            string[] ret = null;
            try
            {
                DataTable dataTable = GetTablesAsDataTable(shouldOpenAndClose);
                if (dataTable != null)
                {
                    for (int i = 0; i < dataTable.Rows.Count; i++)
                    {
                        string table = Convert.ToString(dataTable.Rows[i][0]);
                        if (table != String.Empty)
                        {
                            ret = (string[])ArrayUtils.Add(ret, Convert.ToString(dataTable.Rows[i][0]));
                        }
                    }
                }
            }
            catch (Exception ex)
            {
                string msg = "Error getting tables: " + ex.Message;
                throw new Exception(msg, ex);

            }
            return ret;
        }

        public override DataTable GetTablesAsDataTable()
        {
            return GetTablesAsDataTable(true);
        }

        public override DataTable GetTablesAsDataTable(bool shouldOpenAndClose)
        {
            throw new Exception("Must call GetTablesAsDataTable with database name");
        }

        public override DataTable GetTablesAsDataTable(string databaseName, bool shouldOpenAndClose)
        {
            DataTable ret = new DataTable();

            MongoDBConnection mongoDBConnection  = (MongoDBConnection)QueryManager.Connection;
            if (mongoDBConnection != null)
            {
                MongoServer mongoServer = mongoDBConnection.MongoServer;

                if (mongoServer != null)
                {
                    MongoDatabase database = mongoServer.GetDatabase(databaseName);
                    List<String> names = (List<String>)database.GetCollectionNames();


                    
                    ret.Columns.Add(new DataColumn("name"));

                    foreach (string name in names)
                    {
                        DataRow dataRow = ret.NewRow();
                        dataRow["name"] = name;
                        ret.Rows.Add(dataRow);
                    }
                }
            }

            return ret;
        }

        public override void RemoveTableFromDatabase(string tableName)
        {
            try
            {
                 MongoDBConnection mongoDBConnection  = (MongoDBConnection)QueryManager.Connection;
                 if (mongoDBConnection != null)
                 {
                     MongoServer mongoServer = mongoDBConnection.MongoServer;

                     if (mongoServer != null)
                     {

                         MongoDatabase database = mongoServer.GetDatabase(DefaultDatabase);


                         if (database.CollectionExists(tableName))
                         {
                             database.DropCollection(tableName);
                         }

                     }
                 }
            }
            catch (Exception ex)
            {
                string msg = "Error removing table: " + ex.Message;
                throw new Exception(msg, ex);
            }
        }

        public override void RemoveIndexFromTable(string tableName, string indexName)
        {
            throw new NotImplementedException();
        }

        public override void RemoveSpatialIndexFromTable(string tableName, string indexName)
        {
            throw new NotImplementedException();
        }

        public override void RemoveConstraintFromTable(string tableName, string constraintName)
        {
            throw new NotImplementedException();
        }

        public override string[] GetTableIndexes(string tableName)
        {
            string[] rv = null;
            
            try
            {
                MongoDBConnection mongoDBConnection = (MongoDBConnection)QueryManager.Connection;
                if (mongoDBConnection != null)
                {
                    MongoServer mongoServer = mongoDBConnection.MongoServer;

                    if (mongoServer != null)
                    {

                        MongoDatabase database = mongoServer.GetDatabase(DefaultDatabase);

                        GetIndexesResult indexResult = database.GetCollection(tableName).GetIndexes();
                        for (int i =0; i<indexResult.Count;i++)
                        {
                            rv[i]=indexResult[i].Name;
                            rv = (string[])ArrayUtils.Add(rv, indexResult[i].Name);
                        }

                    }
                }
            }
            catch (Exception ex)
            {
                string msg = "Error GetTableIndexes: " + ex.Message;
                throw new Exception(msg, ex);
            }
            return rv;
           // throw new NotImplementedException();
        }

        public override string[] GetTableIndexes(string tableName, bool shouldOpenAndClose)
        {
            return GetTableIndexes(tableName);
        }

        public override string[] GetTableSpatialIndexes(string tableName)
        {
            return GetTableIndexes(tableName);//TODO check for spacial indexes
        }

        public override string[] GetTableSpatialIndexes(string tableName, bool shouldOpenAndClose)
        {
            return GetTableSpatialIndexes(tableName);

        }

        public override void AddIndexToDatabase(string tableName, string createSQL)
        {
        }
        public override string GetTableClusteredIndex(string tableName)
        {
            //Do Nothing TODO:
            return "";
             throw new NotImplementedException();
        }

        public override string GetTableClusteredIndex(string tableName, bool shouldOpenAndClose)
        {
            return "";
            //Do Nothing TODO:
          //  throw new NotImplementedException();
        }

        public override void AddGeogIndexToDatabase(string tableName)
        {
            try
            {
                MongoDBConnection mongoDBConnection = (MongoDBConnection)QueryManager.Connection;
                if (mongoDBConnection != null)
                {
                    MongoServer mongoServer = mongoDBConnection.MongoServer;

                    if (mongoServer != null)
                    {
                        IMongoIndexKeys keys = new IndexKeysDocument {{ "shapeGeogAsGeoJSON","2dsphere" },{ "type", 1 }};
                        IMongoIndexOptions  opt= new IndexOptionsDocument();
                        mongoServer.GetDatabase(DefaultDatabase).GetCollection(tableName).EnsureIndex(keys, opt);
                    }
                }
            }
            catch (Exception ex)
            {
                string msg = "Error AddGeogIndexToDatabase MongoDB: " + ex.Message;
                throw new Exception(msg, ex);
            }
        }

        public override void AddGeogIndexToDatabase(string tableName, bool shouldOpenCloseConnection)
        {
            AddGeogIndexToDatabase(tableName);
        }
    }
}
