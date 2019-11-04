using System;
using USC.GISResearchLab.Common.Core.Databases;
using USC.GISResearchLab.Common.Databases.ConnectionStringManagers;

//mongodb://[username:password@]host1[:port1][,host2[:port2],...[,hostN[:portN]]][/[database][?options]]
//mongodb://db1.example.net,db2.example.net:2500/?replicaSet=test
namespace TAMU.GeoInnovation.Common.Utils.Databases.MongoDB
{
    public class MongoDBConnectionStringManager : AbstractConnectionStringManager
    {
        public MongoDBConnectionStringManager()
        {
            DatabaseType = DatabaseType.MongoDB;
        }

        public MongoDBConnectionStringManager(string pathToDatabaseDlls, string location, string defualtDatabase, string userName, string password, string[] parameters)
        {
            Location = location;
            DefaultDatabase = defualtDatabase;
            UserName = userName;
            Password = password;
            Parameters = parameters;
            PathToDatabaseDLLs = pathToDatabaseDlls;
        }

        public MongoDBConnectionStringManager(string location, string defualtDatabase, string userName, string password, string[] parameters)
        {
            Location = location;
            DefaultDatabase = defualtDatabase;
            UserName = userName;
            Password = password;
            Parameters = parameters;
        }

        public override string GetConnectionString(DataProviderType dataProviderType)
        {
            string ret = null;
            switch (dataProviderType)
            {
                case DataProviderType.MongoDB:
                    ret = "mongodb://" + UserName + ":" + Password + "@" + Location + ":" + 27017;//"/" + DefaultDatabase;
                                                                                                  // ret = "mongodb://" + Location + ":" +27017 ;
                    break;
                case DataProviderType.Odbc:
                    ret = "";
                    break;
                case DataProviderType.OleDb:
                    ret = "";
                    break;
                default:
                    throw new Exception("Unexpected dataProviderType: " + dataProviderType);
            }
            return ret;
        }
    }
}
