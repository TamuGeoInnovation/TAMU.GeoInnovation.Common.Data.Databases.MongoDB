using System;
using MongoDB.Driver;
using USC.GISResearchLab.Common.Core.Databases;
using USC.GISResearchLab.Common.Databases.ConnectionStringManagers;
using USC.GISResearchLab.Common.Databases.DataSources;
using USC.GISResearchLab.Common.Databases.QueryManagers;

namespace TAMU.GeoInnovation.Common.Utils.Databases.MongoDB
{
    public class MongoDBDataSourceManager : AbstractDataSourceManager
    {
        public MongoDBDataSourceManager()
        {
            ProviderType = DataProviderType.MongoDB;
        }

        public MongoDBDataSourceManager(string pathToDatabaseDlls, string location, string defualtDatabase, string userName, string password, string[] parameters)
        {
            ProviderType = DataProviderType.MongoDB;
            Location = location;
            DefaultDatabase = defualtDatabase;
            UserName = userName;
            Password = password;
            Parameters = parameters;
            PathToDatabaseDLLs = pathToDatabaseDlls;
        }

        public MongoDBDataSourceManager(string location, string defualtDatabase, string userName, string password, string[] parameters)
        {
            ProviderType = DataProviderType.MongoDB;
            Location = location;
            DefaultDatabase = defualtDatabase;
            UserName = userName;
            Password = password;
            Parameters = parameters;
        }

        public override void CreateDatabase(DatabaseType databaseType, string databaseName)
        {
            try
            {
                IConnectionStringManager connectionStringManager = new ConnectionStringManager(DatabaseType.MongoDB, Location, DefaultDatabase, UserName, Password, null);
                string connectionString = connectionStringManager.GetConnectionString(DataProviderType.MongoDB);
                MongoServer mongoServer = MongoServer.Create(connectionString);
                mongoServer.GetDatabase(databaseName);
            }
            catch (Exception ex)
            {
                string msg = "Error creating database: " + ex.Message;
                throw new Exception(msg, ex);
            }
        }


        public override bool Validate(DatabaseType databaseType, string databaseName)
        {
            bool ret = true;
            IQueryManager queryManager = null;
            try
            {
                IConnectionStringManager connectionStringManager = new ConnectionStringManager(PathToDatabaseDLLs, DatabaseType.MongoDB, Location, databaseName, UserName, Password, null);
                string connectionString = connectionStringManager.GetConnectionString(DataProviderType.MongoDB);
                MongoServer mongoServer = new MongoClient(connectionString).GetServer();
            }
            catch (Exception e)
            {
                ret = false;
            }
            return ret;
        }
    }
}
