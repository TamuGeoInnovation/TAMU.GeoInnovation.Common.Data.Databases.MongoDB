using MongoDB.Bson;
using MongoDB.Driver;
using MongoDB.Driver.Builders;
using System;
using System.Data;
using System.Data.SqlClient;
using System.Diagnostics;
using USC.GISResearchLab.Common.Core.Databases;
using USC.GISResearchLab.Common.Databases.ImportStatusManagers;
using USC.GISResearchLab.Common.Databases.QueryManagers;
using USC.GISResearchLab.Common.Databases.SchemaManagers;
using USC.GISResearchLab.Common.Diagnostics.TraceEvents;
using USC.GISResearchLab.Common.Utils.Databases;

namespace TAMU.GeoInnovation.Common.Utils.Databases.MongoDB
{
    public class MongoDBImportStatusManager : AbstractImportStatusManager
    {


        #region Properties
        //MongoDBConnectionStringManager mongoDBConnectionStringManager;
        //MongoServer mongoServer;
        #endregion
        private IQueryManager _QueryManager;
        public IQueryManager QueryManager
        {
            get
            {
                if (_QueryManager == null)
                {
                    _QueryManager = new QueryManager(ApplicationPathToDatabaseDlls, ApplicationDataProviderType, ApplicationDatabaseType, ApplicationConnectionString);
                }
                return _QueryManager;
            }
        }

        //private ISchemaManager _SchemaManager;
        //public ISchemaManager SchemaManager
        //{
        //    get
        //    {
        //        if (_SchemaManager == null)
        //        {
        //            _SchemaManager = new SchemaManager(ApplicationPathToDatabaseDlls, ApplicationDataProviderType, ApplicationDatabaseType, ApplicationConnectionString);
        //        }
        //        return _SchemaManager;
        //    }
        //}

        //#endregion

        public MongoDBImportStatusManager(TraceSource traceSource)
        {
            TraceSource = traceSource;
            //mongoDBConnectionStringManager = new MongoDBConnectionStringManager();
            //mongoServer = MongoServer.Create(mongoDBConnectionStringManager.GetConnectionString(DataProviderType.MongoDB));
        }


        public MongoDBImportStatusManager(DataProviderType providerType)
        {
            ProviderType = providerType;
            //mongoDBConnectionStringManager = new MongoDBConnectionStringManager();
            //mongoServer = MongoServer.Create(mongoDBConnectionStringManager.GetConnectionString(DataProviderType.MongoDB));
        }

        public MongoDBImportStatusManager(DataProviderType providerType, string location, string defaultDatabase, string userName, string password, string[] parameters)
        {
            ProviderType = providerType;
            Location = location;
            DefaultDatabase = defaultDatabase;
            UserName = userName;
            Password = password;
            Parameters = parameters;

            //mongoDBConnectionStringManager = new MongoDBConnectionStringManager();
            //mongoServer = MongoServer.Create(mongoDBConnectionStringManager.GetConnectionString(providerType));
        }

        public MongoDBImportStatusManager(string pathToDatabaseDLLs, DataProviderType providerType, string location, string defaultDatabase, string userName, string password, string[] parameters)
        {
            ProviderType = providerType;
            Location = location;
            DefaultDatabase = defaultDatabase;
            UserName = userName;
            Password = password;
            Parameters = parameters;
            PathToDatabaseDLLs = pathToDatabaseDLLs;

            //mongoDBConnectionStringManager = new MongoDBConnectionStringManager();
            //mongoServer = MongoServer.Create(mongoDBConnectionStringManager.GetConnectionString(providerType));
        }

        public MongoDBImportStatusManager(string pathToDatabaseDlls, DataProviderType providerType, string connectionString)
        {
            SchemaManager = SchemaManagerFactory.GetSchemaManager(pathToDatabaseDlls, providerType, connectionString);
        }


        public override void InitializeConnections()
        {

        }

        public override void CreateStoredProcedures(bool shouldThrowExceptions)
        {
            if (shouldThrowExceptions)
            {
                throw new NotImplementedException("MongoDB does not have Stored Procedure");
            }
        }

        public override void CreateImportStatusStateTable(string tableName, bool restart, bool shouldRemoveStatusTablesFirst)
        {
            try
            {
                MongoDBConnection mongoDBConnection = (MongoDBConnection)SchemaManager.QueryManager.Connection;
                if (mongoDBConnection != null)
                {
                    MongoServer mongoServer = mongoDBConnection.MongoServer;

                    if (mongoServer != null)
                    {

                        MongoDatabase database = mongoServer.GetDatabase(DefaultDatabase);

                        if (restart || shouldRemoveStatusTablesFirst)
                        {
                            if (database.CollectionExists(tableName))
                            {
                                database.DropCollection(tableName);
                            }

                        }

                        CollectionOptionsBuilder options = CollectionOptions.SetAutoIndexId(true);
                        if (!database.CollectionExists(tableName))
                        {
                            database.CreateCollection(tableName, options);
                        }

                        //    SchemaManager.QueryManager.Connection.Open();

                        //    if (restart)
                        //    {
                        //        SchemaManager.RemoveTableFromDatabase(tableName);
                        //    }

                        //    string sql = "use " + SchemaManager.QueryManager.Connection.Database + "; ";
                        //    //sql += "IF NOT EXISTS (SELECT * FROM sysobjects WHERE type = 'U' AND name = '" + tableName + "')";
                        //    sql += "CREATE TABLE " + tableName + " (";
                        //    sql += "id bigint IDENTITY (1,1) NOT NULL,";
                        //    sql += "state varchar(255) DEFAULT NULL,";
                        //    sql += "status varchar(255) DEFAULT NULL,";
                        //    sql += "startDate datetime DEFAULT NULL,";
                        //    sql += "endDate datetime DEFAULT NULL,";
                        //    sql += "message varchar(1000) DEFAULT NULL,";
                        //    sql += "PRIMARY KEY  (id)";
                        //    sql += ");";


                        //    SchemaManager.AddTableToDatabase(tableName, sql);
                        //
                    }
                }
            }
            catch (Exception e)
            {
                string msg = "Error CreateImportStatusStateTable: " + e.Message;

                if (TraceSource != null)
                {
                    TraceSource.TraceEvent(TraceEventType.Error, (int)ExceptionEvents.ExceptionOccurred, msg);
                }

                if (SchemaManager.QueryManager.Connection != null)
                {
                    if (SchemaManager.QueryManager.Connection.State != ConnectionState.Closed)
                    {
                        SchemaManager.QueryManager.Close();
                    }
                }

                throw new Exception(msg, e);
            }
            finally
            {
                if (SchemaManager.QueryManager.Connection != null)
                {
                    if (SchemaManager.QueryManager.Connection.State != ConnectionState.Closed)
                    {
                        SchemaManager.QueryManager.Close();
                    }
                }
            }
        }

        public override void CreateImportStatusCountyTable(string tableName, bool restart, bool shouldRemoveStatusTablesFirst)
        {
            try
            {
                MongoDBConnection mongoDBConnection = (MongoDBConnection)SchemaManager.QueryManager.Connection;
                if (mongoDBConnection != null)
                {
                    MongoServer mongoServer = mongoDBConnection.MongoServer;

                    if (mongoServer != null)
                    {

                        MongoDatabase database = mongoServer.GetDatabase(DefaultDatabase);

                        if (restart || shouldRemoveStatusTablesFirst)
                        {
                            if (database.CollectionExists(tableName))
                            {
                                database.DropCollection(tableName);
                            }

                        }

                        CollectionOptionsBuilder options = CollectionOptions.SetAutoIndexId(true);
                        if (!database.CollectionExists(tableName))
                        {
                            database.CreateCollection(tableName, options);
                        }

                        //    string sql = "use " + SchemaManager.QueryManager.Connection.Database + "; ";
                        //    //sql += "IF NOT EXISTS (SELECT * FROM sysobjects WHERE type = 'U' AND name = '" + tableName + "')";
                        //    sql += "CREATE TABLE " + tableName + " (";
                        //    sql += "id bigint IDENTITY (1,1) NOT NULL,";
                        //    sql += "state varchar(255) DEFAULT NULL,";
                        //    sql += "county varchar(255) DEFAULT NULL,";
                        //    sql += "status varchar(255) DEFAULT NULL,";
                        //    sql += "startDate datetime DEFAULT NULL,";
                        //    sql += "endDate datetime DEFAULT NULL,";
                        //    sql += "message varchar(1000) DEFAULT NULL,";
                        //    sql += "PRIMARY KEY  (id)";
                        //    sql += ");";

                        //    SchemaManager.AddTableToDatabase(tableName, sql);
                        //
                    }
                }
            }
            catch (Exception e)
            {
                string msg = "Error CreateImportStatusCountyTable: " + e.Message;

                if (TraceSource != null)
                {
                    TraceSource.TraceEvent(TraceEventType.Error, (int)ExceptionEvents.ExceptionOccurred, msg);
                }

                if (SchemaManager.QueryManager.Connection != null)
                {
                    if (SchemaManager.QueryManager.Connection.State != ConnectionState.Closed)
                    {
                        SchemaManager.QueryManager.Close();
                    }
                }

                throw new Exception(msg, e);
            }
            finally
            {
                if (SchemaManager.QueryManager.Connection != null)
                {
                    if (SchemaManager.QueryManager.Connection.State != ConnectionState.Closed)
                    {
                        SchemaManager.QueryManager.Close();
                    }
                }
            }
        }

        public override void CreateImportStatusFileTable(string tableName, bool restart, bool shouldRemoveStatusTablesFirst)
        {
            try
            {
                MongoDBConnection mongoDBConnection = (MongoDBConnection)SchemaManager.QueryManager.Connection;
                if (mongoDBConnection != null)
                {
                    MongoServer mongoServer = mongoDBConnection.MongoServer;

                    if (mongoServer != null)
                    {

                        MongoDatabase database = mongoServer.GetDatabase(DefaultDatabase);

                        if (String.IsNullOrEmpty(mongoDBConnection.Database))
                        {
                            mongoDBConnection.Database = DefaultDatabase;
                        }


                        if (restart || shouldRemoveStatusTablesFirst)
                        {
                            if (database.CollectionExists(tableName))
                            {
                                database.DropCollection(tableName);
                            }

                        }

                        CollectionOptionsBuilder options = CollectionOptions.SetAutoIndexId(true);
                        if (!database.CollectionExists(tableName))
                        {
                            database.CreateCollection(tableName, options);
                        }

                    }
                }
                //string sql = "use " + SchemaManager.QueryManager.Connection.Database + "; ";
                ////sql += "IF NOT EXISTS (SELECT * FROM sysobjects WHERE type = 'U' AND name = '" + tableName + "')";
                //sql += "CREATE TABLE " + tableName + " (";
                //sql += "id bigint NOT NULL AUTO_INCREMENT,";
                //sql += "state varchar(255) DEFAULT NULL,";
                //sql += "county varchar(255) DEFAULT NULL,";
                //sql += "filename varchar(255) DEFAULT NULL,";
                //sql += "status varchar(255) DEFAULT NULL,";
                //sql += "startDate datetime DEFAULT NULL,";
                //sql += "endDate datetime DEFAULT NULL,";
                //sql += "message varchar(1000) DEFAULT NULL,";
                //sql += "PRIMARY KEY  (id)";
                //sql += ");";

                //SchemaManager.AddTableToDatabase(tableName, sql, false);
            }
            catch (Exception e)
            {
                string msg = "Error CreateImportStatusFileCollection: " + e.Message;

                if (TraceSource != null)
                {
                    TraceSource.TraceEvent(TraceEventType.Error, (int)ExceptionEvents.ExceptionOccurred, msg);
                }

                if (SchemaManager.QueryManager.Connection != null)
                {
                    if (SchemaManager.QueryManager.Connection.State != ConnectionState.Closed)
                    {
                        SchemaManager.QueryManager.Close();
                    }
                }

                throw new Exception(msg, e);
            }
            finally
            {
                if (SchemaManager.QueryManager.Connection != null)
                {
                    if (SchemaManager.QueryManager.Connection.State != ConnectionState.Closed)
                    {
                        SchemaManager.QueryManager.Close();
                    }
                }
            }
        }

        public override bool CheckStatusStateAlreadyDone(string tableName, string state)
        {
            bool ret = false;

            try
            {

                if (TraceSource != null)
                {
                    TraceSource.TraceEvent(TraceEventType.Verbose, (int)ProcessEvents.Completing, "checking state status: " + state);
                }

                MongoDBConnection mongoDBConnection = (MongoDBConnection)SchemaManager.QueryManager.Connection;
                if (mongoDBConnection != null)
                {
                    MongoServer mongoServer = mongoDBConnection.MongoServer;

                    if (mongoServer != null)
                    {

                        MongoDatabase database = mongoServer.GetDatabase(DefaultDatabase);
                        MongoCollection mongoCollection = database.GetCollection<BsonDocument>(tableName);


                        int count = (int)mongoCollection.FindAs<BsonDocument>(Query.And(Query.EQ("state", state), Query.EQ("status", "Finished"))).Count();
                        if (count > 0)
                        {
                            ret = true;
                        }

                        //string sql = "select id FROM " + tableName + "";
                        //sql += " where ";
                        //sql += " state=?state";
                        //sql += " and ";
                        //sql += " status='Finished'";

                        //SqlCommand cmd = new SqlCommand(sql);
                        //cmd.Parameters.Add(SqlParameterUtils.BuildSqlParameter("?state", SqlDbType.VarChar, state));

                        //SchemaManager.QueryManager.AddParameters(cmd.Parameters);
                        //int id = SchemaManager.QueryManager.ExecuteScalarInt(CommandType.Text, cmd.CommandText, true);

                        //if (id > 0)
                        //{
                        //    ret = true;
                        //}

                    }
                }
            }
            catch (Exception exc)
            {
                string msg = "Error checking state status: " + state;

                if (TraceSource != null)
                {
                    TraceSource.TraceEvent(TraceEventType.Error, (int)ExceptionEvents.ExceptionOccurred, msg);
                }

                throw new Exception(msg, exc);
            }
            return ret;
        }

        public override bool CheckStatusCountyAlreadyDone(string tableName, string county)
        {
            bool ret = false;

            try
            {

                if (TraceSource != null)
                {
                    TraceSource.TraceEvent(TraceEventType.Verbose, (int)ProcessEvents.Completing, "checking county status: " + county);
                }

                MongoDBConnection mongoDBConnection = (MongoDBConnection)SchemaManager.QueryManager.Connection;
                if (mongoDBConnection != null)
                {
                    MongoServer mongoServer = mongoDBConnection.MongoServer;

                    if (mongoServer != null)
                    {

                        MongoDatabase database = mongoServer.GetDatabase(DefaultDatabase);
                        MongoCollection mongoCollection = database.GetCollection<BsonDocument>(tableName);


                        int count = (int)mongoCollection.FindAs<BsonDocument>(Query.And(Query.EQ("county", county), Query.EQ("status", "Finished"))).Count();
                        if (count > 0)
                        {
                            ret = true;
                        }
                        //string sql = "select id FROM " + tableName + "";
                        //sql += " where ";
                        //sql += " county=?county";
                        //sql += " and ";
                        //sql += " status='Finished'";

                        //SqlCommand cmd = new SqlCommand(sql);
                        //cmd.Parameters.Add(SqlParameterUtils.BuildSqlParameter("?county", SqlDbType.VarChar, county));

                        //SchemaManager.QueryManager.AddParameters(cmd.Parameters);
                        //int id = SchemaManager.QueryManager.ExecuteScalarInt(CommandType.Text, cmd.CommandText, true);

                        //if (id > 0)
                        //{
                        //    ret = true;
                        //}
                    }
                }

            }
            catch (Exception exc)
            {
                string msg = "Error checking county status: " + county;

                if (TraceSource != null)
                {
                    TraceSource.TraceEvent(TraceEventType.Error, (int)ExceptionEvents.ExceptionOccurred, msg);
                }

                throw new Exception(msg, exc);
            }
            return ret;
        }

        public override bool CheckStatusFileAlreadyDone(string tableName, string state, string county, string file)
        {
            bool ret = false;

            try
            {

                if (TraceSource != null)
                {
                    TraceSource.TraceEvent(TraceEventType.Verbose, (int)ProcessEvents.Completing, "checking file status: " + file);
                }

                MongoDBConnection mongoDBConnection = (MongoDBConnection)SchemaManager.QueryManager.Connection;
                if (mongoDBConnection != null)
                {
                    MongoServer mongoServer = mongoDBConnection.MongoServer;

                    if (mongoServer != null)
                    {

                        MongoDatabase database = mongoServer.GetDatabase(DefaultDatabase);
                        MongoCollection mongoCollection = database.GetCollection<BsonDocument>(tableName);


                        int count = (int)mongoCollection.FindAs<BsonDocument>(Query.And(Query.EQ("county", county), Query.EQ("state", state), Query.EQ("status", "Finished"))).Count();
                        if (count > 0)
                        {
                            ret = true;
                        }
                        //string sql = "select id FROM " + tableName + "";
                        //sql += " where ";
                        //sql += " filename=?filename";
                        //sql += " and ";
                        //sql += " state=?state";
                        //sql += " and ";
                        //sql += " county=?county";
                        //sql += " and ";
                        //sql += " status='Finished'";

                        //SqlCommand cmd = new SqlCommand(sql);
                        //cmd.Parameters.Add(SqlParameterUtils.BuildSqlParameter("?filename", SqlDbType.VarChar, file));
                        //cmd.Parameters.Add(SqlParameterUtils.BuildSqlParameter("?state", SqlDbType.VarChar, state));
                        //cmd.Parameters.Add(SqlParameterUtils.BuildSqlParameter("?county", SqlDbType.VarChar, county));

                        //SchemaManager.QueryManager.AddParameters(cmd.Parameters);
                        //int id = SchemaManager.QueryManager.ExecuteScalarInt(CommandType.Text, cmd.CommandText, true);

                        //if (id > 0)
                        //{
                        //    ret = true;
                        //}

                    }
                }
            }
            catch (Exception exc)
            {
                string msg = "Error checking file status: " + file;

                if (TraceSource != null)
                {
                    TraceSource.TraceEvent(TraceEventType.Error, (int)ExceptionEvents.ExceptionOccurred, msg);
                }

                throw new Exception(msg, exc);
            }
            return ret;
        }



        public override bool UpdateStatusFile(string tableName, string state, string county, string file, Statuses status, string message)
        {
            bool ret = false;

            try
            {

                if (TraceSource != null)
                {
                    TraceSource.TraceEvent(TraceEventType.Verbose, (int)ProcessEvents.Completing, "updating file status: " + file + " status: " + status);
                }

                MongoDBConnection mongoDBConnection = (MongoDBConnection)SchemaManager.QueryManager.Connection;
                if (mongoDBConnection != null)
                {
                    MongoServer mongoServer = mongoDBConnection.MongoServer;

                    if (mongoServer != null)
                    {

                        MongoDatabase database = mongoServer.GetDatabase(DefaultDatabase);
                        MongoCollection mongoCollection = database.GetCollection<BsonDocument>(tableName);

                        var update = Update.Set("status", GetStatusString(status)).Set("endDate", DateTime.Now.ToString()).Set("message", message);
                        mongoCollection.Update(Query.And(Query.EQ("county", county), Query.EQ("state", state), Query.EQ("filename", file)), update);


                        //string sql = "select id FROM " + tableName + "";
                        //sql += " where ";
                        //sql += " filename=?filename";
                        //sql += " and ";
                        //sql += " county=?county";
                        //sql += " and ";
                        //sql += " state=?state";

                        //SqlCommand cmd = new SqlCommand(sql);
                        //cmd.Parameters.Add(SqlParameterUtils.BuildSqlParameter("?filename", SqlDbType.VarChar, file));
                        //cmd.Parameters.Add(SqlParameterUtils.BuildSqlParameter("?county", SqlDbType.VarChar, county));
                        //cmd.Parameters.Add(SqlParameterUtils.BuildSqlParameter("?state", SqlDbType.VarChar, state));

                        //SchemaManager.QueryManager.AddParameters(cmd.Parameters);
                        //int id = SchemaManager.QueryManager.ExecuteScalarInt(CommandType.Text, cmd.CommandText, true);

                        //if (id <= 0 || status == Statuses.start)
                        //{
                        //    InsertStatusFile(tableName, state, county,  file);
                        //}
                        //else
                        //{
                        //    sql = "update " + tableName;
                        //    sql += " set ";
                        //    sql += " status=?status,";
                        //    sql += " endDate=?endDate,";
                        //    sql += " message=?message";
                        //    sql += " where ";
                        //    sql += " id=?id ";

                        //    cmd = new SqlCommand(sql);
                        //    cmd.Parameters.Add(SqlParameterUtils.BuildSqlParameter("?status", SqlDbType.VarChar, GetStatusString(status)));
                        //    cmd.Parameters.Add(SqlParameterUtils.BuildSqlParameter("?endDate", SqlDbType.DateTime, DateTime.Now));
                        //    cmd.Parameters.Add(SqlParameterUtils.BuildSqlParameter("?message", SqlDbType.VarChar, message));
                        //    cmd.Parameters.Add(SqlParameterUtils.BuildSqlParameter("?id", SqlDbType.BigInt, id));

                        //    SchemaManager.QueryManager.AddParameters(cmd.Parameters);
                        //    SchemaManager.QueryManager.ExecuteNonQuery(CommandType.Text, cmd.CommandText, true);
                        //}


                        ret = true;
                    }
                }

            }
            catch (Exception exc)
            {
                string msg = "Error updating file status: " + file + " status: " + status + " - " + exc.Message;
                if (TraceSource != null)
                {
                    TraceSource.TraceEvent(TraceEventType.Error, (int)ExceptionEvents.ExceptionOccurred, msg);
                }
                throw new Exception(msg, exc);
            }
            return ret;
        }



        public override bool UpdateStatusState(string tableName, string state, Statuses status, string message)
        {
            bool ret = false;

            try
            {
                if (TraceSource != null)
                {
                    TraceSource.TraceEvent(TraceEventType.Verbose, (int)ProcessEvents.Completing, "updating state status: " + state + " status: " + status);
                }

                MongoDBConnection mongoDBConnection = (MongoDBConnection)SchemaManager.QueryManager.Connection;
                if (mongoDBConnection != null)
                {
                    MongoServer mongoServer = mongoDBConnection.MongoServer;

                    if (mongoServer != null)
                    {

                        MongoDatabase database = mongoServer.GetDatabase(DefaultDatabase);
                        MongoCollection mongoCollection = database.GetCollection<BsonDocument>(tableName);

                        if (String.IsNullOrEmpty(message))
                        {
                            message = "";
                        }

                        string statusString = GetStatusString(status);
                        if (String.IsNullOrEmpty(statusString))
                        {
                            statusString = "";
                        }

                        bool stateExists = CheckStatusStateAlreadyDone(tableName, state);
                        if (!stateExists)
                        {
                            BsonDocument bsonDocument = new BsonDocument();
                            bsonDocument.Add("state", state);
                            bsonDocument.Add("status", "");
                            bsonDocument.Add("startDate", "");
                            bsonDocument.Add("endDate", "");
                            bsonDocument.Add("message", "");
                            mongoCollection.Insert(bsonDocument);
                        }

                        var update = Update.Set("status", statusString).Set("endDate", DateTime.Now.ToString()).Set("message", message);
                        mongoCollection.Update(Query.EQ("state", state), update);


                        //string sql = "select id FROM " + tableName + "";
                        //sql += " where ";
                        //sql += " state=?state";

                        //SqlCommand cmd = new SqlCommand(sql);
                        //cmd.Parameters.Add(SqlParameterUtils.BuildSqlParameter("?state", SqlDbType.VarChar, state));

                        //SchemaManager.QueryManager.AddParameters(cmd.Parameters);
                        //int id = SchemaManager.QueryManager.ExecuteScalarInt(CommandType.Text, cmd.CommandText, true);

                        //if (id <= 0 || status == Statuses.start)
                        //{
                        //    InsertStatusState(tableName, state);
                        //}
                        //else
                        //{
                        //    sql = "update " + tableName + "";
                        //    sql += " set ";
                        //    sql += " status=?status,";
                        //    sql += " endDate=?endDate,";
                        //    sql += " message=?message";
                        //    sql += " where ";
                        //    sql += " id=?id ";

                        //    cmd = new SqlCommand(sql);
                        //    cmd.Parameters.Add(SqlParameterUtils.BuildSqlParameter("?status", SqlDbType.VarChar, GetStatusString(status)));
                        //    cmd.Parameters.Add(SqlParameterUtils.BuildSqlParameter("?endDate", SqlDbType.DateTime, DateTime.Now));
                        //    cmd.Parameters.Add(SqlParameterUtils.BuildSqlParameter("?message", SqlDbType.VarChar, message));
                        //    cmd.Parameters.Add(SqlParameterUtils.BuildSqlParameter("?id", SqlDbType.BigInt, id));

                        //    SchemaManager.QueryManager.AddParameters(cmd.Parameters);
                        //    SchemaManager.QueryManager.ExecuteNonQuery(CommandType.Text, cmd.CommandText, true);
                        //}

                        ret = true;
                    }
                }

            }
            catch (Exception exc)
            {
                string msg = "Error updating state status: " + state + " status: " + status + ":" + exc.Message;
                if (TraceSource != null)
                {
                    TraceSource.TraceEvent(TraceEventType.Error, (int)ExceptionEvents.ExceptionOccurred, msg);
                }
                throw new Exception(msg, exc);
            }
            return ret;
        }



        public override bool UpdateStatusCounty(string tableName, string state, string county, Statuses status, string message)
        {
            bool ret = false;

            try
            {

                if (TraceSource != null)
                {
                    TraceSource.TraceEvent(TraceEventType.Verbose, (int)ProcessEvents.Completing, "updating county status: " + county + " status: " + status);
                }

                MongoDBConnection mongoDBConnection = (MongoDBConnection)SchemaManager.QueryManager.Connection;
                if (mongoDBConnection != null)
                {
                    MongoServer mongoServer = mongoDBConnection.MongoServer;

                    if (mongoServer != null)
                    {

                        MongoDatabase database = mongoServer.GetDatabase(DefaultDatabase);
                        MongoCollection mongoCollection = database.GetCollection<BsonDocument>(tableName);

                        var update = Update.Set("status", GetStatusString(status)).Set("endDate", DateTime.Now.ToString()).Set("message", message);
                        mongoCollection.Update(Query.And(Query.EQ("county", county), Query.EQ("state", state)), update);

                        //BsonDocument document = mongoCollection.FindOneAs(query);
                        //document.Add("state", state);
                        //document.Add("county", county);
                        //document.Add("filename", file);
                        //document.Add("status", "Started");
                        //document.Add("startDate", DateTime.Now.ToString());
                        //mongoCollection.Insert(document);
                        //ret = true;

                        //string sql = "select id FROM " + tableName + "";
                        //sql += " where ";
                        //sql += " state=?state";
                        //sql += " and ";
                        //sql += " county=?county";

                        //SqlCommand cmd = new SqlCommand(sql);
                        //cmd.Parameters.Add(SqlParameterUtils.BuildSqlParameter("?state", SqlDbType.VarChar, state));
                        //cmd.Parameters.Add(SqlParameterUtils.BuildSqlParameter("?county", SqlDbType.VarChar, county));

                        //SchemaManager.QueryManager.AddParameters(cmd.Parameters);
                        //int id = SchemaManager.QueryManager.ExecuteScalarInt(CommandType.Text, cmd.CommandText, true);



                        //if (id <= 0 || status == Statuses.start)
                        //{
                        //    InsertStatusCounty(tableName, state, county);
                        //}
                        //else
                        //{
                        //    sql = "update " + tableName + "";
                        //    sql += " set ";
                        //    sql += " status=?status,";
                        //    sql += " endDate=?endDate,";
                        //    sql += " message=?message";
                        //    sql += " where ";
                        //    sql += " id=?id ";

                        //    cmd = new SqlCommand(sql);
                        //    cmd.Parameters.Add(SqlParameterUtils.BuildSqlParameter("?status", SqlDbType.VarChar, GetStatusString(status)));
                        //    cmd.Parameters.Add(SqlParameterUtils.BuildSqlParameter("?endDate", SqlDbType.DateTime, DateTime.Now));
                        //    cmd.Parameters.Add(SqlParameterUtils.BuildSqlParameter("?message", SqlDbType.VarChar, message));
                        //    cmd.Parameters.Add(SqlParameterUtils.BuildSqlParameter("?id", SqlDbType.BigInt, id));

                        //    SchemaManager.QueryManager.AddParameters(cmd.Parameters);
                        //    SchemaManager.QueryManager.ExecuteNonQuery(CommandType.Text, cmd.CommandText, true);
                        //}

                        ret = true;
                    }
                }

            }
            catch (Exception exc)
            {
                string msg = "Error updating county status: " + county + " status: " + status + ":" + exc.Message;

                if (TraceSource != null)
                {
                    TraceSource.TraceEvent(TraceEventType.Error, (int)ExceptionEvents.ExceptionOccurred, msg);
                }

                throw new Exception(msg, exc);
            }
            return ret;
        }

        public override bool InsertStatusFile(string tableName, string state, string county, string file)
        {
            bool ret = false;

            try
            {

                if (TraceSource != null)
                {
                    TraceSource.TraceEvent(TraceEventType.Verbose, (int)ProcessEvents.Completing, "inserting file status: " + file + " status: ");
                }
                //string sql = "";
                //sql += "db." + tableName + ".insert ";
                //sql += tableName + ",";
                //sql += " { ";
                //sql += " state: \"" + state + "\", ";
                //sql += " county: \"" + county  + "\", ";
                //sql += " filename:\"" + file+ "\", ";
                //sql += " status:\"" + "Started" + "\", ";
                //sql += " startDate\"" + DateTime.Now.ToString() + "\" ";
                //sql += " } ";
                //sql += " )";

                MongoDBConnection mongoDBConnection = (MongoDBConnection)SchemaManager.QueryManager.Connection;
                if (mongoDBConnection != null)
                {
                    MongoServer mongoServer = mongoDBConnection.MongoServer;

                    if (mongoServer != null)
                    {

                        MongoDatabase database = mongoServer.GetDatabase(DefaultDatabase);
                        MongoCollection mongoCollection = database.GetCollection<BsonDocument>(tableName);
                        BsonDocument document = new BsonDocument();
                        document.Add("state", state);
                        document.Add("county", county);
                        document.Add("filename", file);
                        document.Add("status", "Started");
                        document.Add("startDate", DateTime.Now.ToString());
                        document.Add("endDate", "");
                        document.Add("message", "");
                        mongoCollection.Insert(document);
                        ret = true;
                    }
                }

            }
            catch (Exception exc)
            {
                string msg = "Error inserting file status: " + file + " status: " + ":" + exc.Message;

                if (TraceSource != null)
                {
                    TraceSource.TraceEvent(TraceEventType.Error, (int)ExceptionEvents.ExceptionOccurred, msg);
                }

                throw new Exception(msg, exc);
            }
            return ret;
        }

        public override bool InsertStatusState(string tableName, string state)
        {
            bool ret = false;

            try
            {

                if (TraceSource != null)
                {
                    TraceSource.TraceEvent(TraceEventType.Verbose, (int)ProcessEvents.Completing, "inserting state status: " + state + " status: ");
                }

                string sql = "INSERT into " + tableName + "";
                sql += " (";
                sql += " state,";
                sql += " status,";
                sql += " startDate";
                sql += " )";
                sql += " VALUES ";
                sql += " (";
                sql += " ?state,";
                sql += " ?status,";
                sql += " ?startDate";
                sql += " )";


                SqlCommand cmd = new SqlCommand(sql);
                cmd.Parameters.Add(SqlParameterUtils.BuildSqlParameter("state", SqlDbType.VarChar, state));
                cmd.Parameters.Add(SqlParameterUtils.BuildSqlParameter("?status", SqlDbType.VarChar, "Started"));
                cmd.Parameters.Add(SqlParameterUtils.BuildSqlParameter("?startDate", SqlDbType.DateTime, DateTime.Now));

                SchemaManager.QueryManager.AddParameters(cmd.Parameters);
                SchemaManager.QueryManager.ExecuteNonQuery(CommandType.Text, cmd.CommandText, true);


                ret = true;

            }
            catch (Exception exc)
            {
                string msg = "Error inserting state status: " + state + " status: " + ":" + exc.Message;

                if (TraceSource != null)
                {
                    TraceSource.TraceEvent(TraceEventType.Error, (int)ExceptionEvents.ExceptionOccurred, msg);
                }

                throw new Exception(msg, exc);
            }
            return ret;
        }

        public override bool InsertStatusCounty(string tableName, string state, string county)
        {
            bool ret = false;

            try
            {

                if (TraceSource != null)
                {
                    TraceSource.TraceEvent(TraceEventType.Verbose, (int)ProcessEvents.Completing, "inserting county status: " + county + " status: ");
                }

                string sql = "INSERT into " + tableName + "";
                sql += " (";
                sql += " state,";
                sql += " county,";
                sql += " status,";
                sql += " startDate";
                sql += " )";
                sql += " VALUES ";
                sql += " (";
                sql += " ?state,";
                sql += " ?county,";
                sql += " ?status,";
                sql += " ?startDate";
                sql += " )";


                SqlCommand cmd = new SqlCommand(sql);
                cmd.Parameters.Add(SqlParameterUtils.BuildSqlParameter("?state", SqlDbType.VarChar, state));
                cmd.Parameters.Add(SqlParameterUtils.BuildSqlParameter("?county", SqlDbType.VarChar, county));
                cmd.Parameters.Add(SqlParameterUtils.BuildSqlParameter("?status", SqlDbType.VarChar, "Started"));
                cmd.Parameters.Add(SqlParameterUtils.BuildSqlParameter("?startDate", SqlDbType.DateTime, DateTime.Now));

                SchemaManager.QueryManager.AddParameters(cmd.Parameters);
                SchemaManager.QueryManager.ExecuteNonQuery(CommandType.Text, cmd.CommandText, true);


                ret = true;

            }
            catch (Exception exc)
            {
                string msg = "Error inserting county status: " + county + " status: " + ":" + exc.Message;

                if (TraceSource != null)
                {
                    TraceSource.TraceEvent(TraceEventType.Error, (int)ExceptionEvents.ExceptionOccurred, msg);
                }

                throw new Exception(msg, exc);
            }
            return ret;
        }
    }
}
