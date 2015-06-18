using System;
using System.Data;
using System.Data.SqlClient;
using MongoDB.Driver;
using USC.GISResearchLab.Common.Core.Databases;

namespace TAMU.GeoInnovation.Common.Utils.Databases.MongoDB
{
    public class MongoDBConnection : IDbConnection
    {
        #region

        private MongoServer _MongoServer;
        public MongoServer MongoServer
        {
            get
            {
                if (_MongoServer == null)
                {
                    if (!String.IsNullOrEmpty(ConnectionString))
                    {
                        MongoDBConnectionStringManager connectionStringManager = new MongoDBConnectionStringManager();
                        connectionStringManager.Location = ConnectionString;
                        _MongoServer = MongoServer.Create(ConnectionString);
                    }
                }

                return _MongoServer;
            }
            set
            {
                _MongoServer = value;
            }
        }

        public string Database { get; set; }
        public string DataSource { get; set; }

        #endregion
        public MongoDBConnection()
        {
            try
            {
                if (!String.IsNullOrEmpty(ConnectionString))
                {
                    MongoDBConnectionStringManager connectionStringManager = new MongoDBConnectionStringManager();
                    connectionStringManager.Location = ConnectionString;
                    MongoServer = MongoServer.Create(connectionStringManager.GetConnectionString(DataProviderType.MongoDB));

                }
            }
            catch (Exception e)
            {
                throw new Exception("Unable to create new MongoDBConnection - " + e.Message);
            }
        }

        
        public void Close()
        {
            MongoServer.Disconnect();
        }

        public void Open() {

            MongoServer.Connect();
        }

        public IDbTransaction BeginTransaction(IsolationLevel il)
        {
            // do nothing
            return null;
            
        }

        public IDbTransaction BeginTransaction()
        {
            // do nothing
            return null;
        }

        public void ChangeDatabase(string databaseName)
        {
            // do nother
        }

        public string ConnectionString
        {
            get;
            set;
        }

        public int ConnectionTimeout
        {
            get
            {
                int ret = 100;
                if (MongoServer != null)
                {
                    ret = Convert.ToInt32(MongoServer.Settings.ConnectTimeout.TotalMilliseconds);
                }
                return ret;
            }
        }

        public IDbCommand CreateCommand()
        {
            // do nothing
            return new SqlCommand();
        }

        public ConnectionState State
        {
            get
            {
                ConnectionState ret = ConnectionState.Broken;
                switch (MongoServer.State)
                {
                    case MongoServerState.Connected:
                        ret = ConnectionState.Open;
                        break;
                    case MongoServerState.ConnectedToSubset:
                        ret = ConnectionState.Open;
                        break;
                    case MongoServerState.Connecting:
                        ret = ConnectionState.Connecting;
                        break;
                    case MongoServerState.Disconnected:
                        ret = ConnectionState.Closed;
                        break;
                    case MongoServerState.Disconnecting:
                        ret = ConnectionState.Open;
                        break;

                        // dan - was causing it not to compile
                    //case MongoServerState.Unknown:
                    //    ret = ConnectionState.Broken;
                    //    break;

                }

                return ret;
            }
        }

        public void Dispose()
        {
            if (MongoServer != null)
            {
                MongoServer.Disconnect();
            }
        }
    }
}
