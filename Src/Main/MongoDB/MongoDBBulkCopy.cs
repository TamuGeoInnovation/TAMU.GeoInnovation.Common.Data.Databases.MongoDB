using MongoDB.Bson;
using MongoDB.Driver;
using MongoDB.Driver.GeoJsonObjectModel;
using System;
using System.Collections;
using System.Collections.Generic;
using System.Data;
using USC.GISResearchLab.Common.Core.Databases.BulkCopys;
using USC.GISResearchLab.Common.Databases.QueryManagers;
using USC.GISResearchLab.Common.Geometries.Points;
using USC.GISResearchLab.Common.Geometries.Polygons;

namespace TAMU.GeoInnovation.Common.Utils.Databases.MongoDB
{
    public class MongoDBBulkCopy : AbstractBulkCopy
    {

        public bool ShouldIncludeMultiPolygons { get; set; }

        public MongoDBBulkCopy()
        {

        }


        public MongoDBBulkCopy(IQueryManager queryManager)
        {
            QueryManager = queryManager;
        }


        public override void Close()
        {
            throw new NotImplementedException();
        }

        public override void GenerateColumnMappings()
        {
            //throw new NotImplementedException();
        }

        public override void GenerateColumnMappings(string[] excludeColumns)
        {
            //throw new NotImplementedException();
        }

        public override void WriteToServer(DataRow[] rows)
        {
            throw new NotImplementedException();
        }

        public override void WriteToServer(DataTable dataTable)
        {
            WriteToServer(dataTable.CreateDataReader());
        }

        public override void WriteToServer(DataTable dataTable, DataRowState dataRowState)
        {
            WriteToServer(dataTable.CreateDataReader());
        }

        public override void WriteToServer(IDataReader dataReader)
        {

            MongoDBConnection mongoDBConnection = (MongoDBConnection)QueryManager.Connection;
            if (mongoDBConnection != null)
            {
                MongoServer mongoServer = mongoDBConnection.MongoServer;

                if (mongoServer != null)
                {
                    MongoDatabase database = mongoServer.GetDatabase(DatabaseName);
                    MongoCollection collection = database.GetCollection(DestinationTableName);
                    //Implement veerything related to MongoInsert

                    ////Extract the colomn names
                    Dictionary<int, string> databaseField = new Dictionary<int, string>();
                    int i = 0;
                    foreach (DataRow dataRow in SchemaDataTable.Rows)
                    {
                        databaseField.Add(i, (string)dataRow[0]);
                        i++;
                    }


                    int j = 0;
                    //insert data  in bsoncolomn
                    while (dataReader.Read())
                    {


                        BsonDocument bsonDocument = new BsonDocument();
                        // GeoJsonObject geoJsonObject = new GeoJsonObject;

                        if (dataReader["shapeGeogAsGeoJSON"] != null)
                        {
                            List<Polygon> geogJson = (List<Polygon>)dataReader["shapeGeogAsGeoJSON"];
                            List<Polygon> geomJson = (List<Polygon>)dataReader["shapeGeomAsGeoJSON"];

                            if (geogJson != null && geomJson != null)
                            {
                                if (geogJson.Count > 0 && geomJson.Count > 0)
                                {
                                    i = -1;
                                    foreach (DataRow dataRow in SchemaDataTable.Rows)
                                    {
                                        //bsonDocument.Add("_id", BsonValue.Create(BsonType.ObjectId));
                                        i++;
                                        string columnName = databaseField[i];

                                        if (String.Compare(columnName, "shapeGeogAsGeoJSON", true) != 0 && String.Compare(columnName, "shapeGeomAsGeoJSON", true) != 0)
                                        {

                                            string value = Convert.ToString(dataReader.GetValue(dataReader.GetOrdinal(columnName)));
                                            bsonDocument.Add(columnName, value);
                                        }
                                        else
                                        {

                                            List<Polygon> polygons = (List<Polygon>)dataReader[columnName];

                                            if (polygons.Count > 0)
                                            {
                                                if (polygons.Count == 1)
                                                {
                                                    Polygon polygon = polygons[0];

                                                    List<GeoJson2DCoordinates> coordinates = new List<GeoJson2DCoordinates>();

                                                    for (int m = 0; m < polygon.Points.Length - 1; m++)
                                                    {
                                                        Point point = polygon.Points[m];
                                                        GeoJson2DCoordinates geoJson2DCoordinates = new GeoJson2DCoordinates(point.X, point.Y);
                                                        coordinates.Add(geoJson2DCoordinates);
                                                    }

                                                    GeoJsonPolygon<GeoJson2DCoordinates> geoJsonPolygon = GeoJson.Polygon(coordinates.ToArray());

                                                    bsonDocument.Add(columnName, geoJsonPolygon.ToBsonDocument());
                                                }
                                                else
                                                {
                                                    if (ShouldIncludeMultiPolygons)
                                                    {
                                                        ArrayList geoJsonPolygons = new ArrayList();
                                                        List<GeoJsonPolygonCoordinates<GeoJson2DCoordinates>> lists = new List<GeoJsonPolygonCoordinates<GeoJson2DCoordinates>>();

                                                        for (int k = 0; k < polygons.Count; k++)
                                                        {
                                                            Polygon polygon = polygons[k];

                                                            List<GeoJson2DCoordinates> coordinates = new List<GeoJson2DCoordinates>();

                                                            for (int m = 0; m < polygon.Points.Length - 1; m++)
                                                            {
                                                                Point point = polygon.Points[m];
                                                                GeoJson2DCoordinates geoJson2DCoordinate = new GeoJson2DCoordinates(point.X, point.Y);
                                                                coordinates.Add(geoJson2DCoordinate);
                                                            }

                                                            GeoJsonLinearRingCoordinates<GeoJson2DCoordinates> ring = new GeoJsonLinearRingCoordinates<GeoJson2DCoordinates>(coordinates);
                                                            GeoJsonPolygonCoordinates<GeoJson2DCoordinates> coordinateLists = new GeoJsonPolygonCoordinates<GeoJson2DCoordinates>(ring);
                                                            lists.Add(coordinateLists);


                                                        }

                                                        GeoJsonMultiPolygon<GeoJson2DCoordinates> geoJsonMultiPolygon = GeoJson.MultiPolygon(lists.ToArray());
                                                        bsonDocument.Add(columnName, geoJsonMultiPolygon.ToBsonDocument());
                                                    }
                                                    else
                                                    {
                                                        // only insert the first polygon
                                                        Polygon polygon = polygons[0];

                                                        List<GeoJson2DCoordinates> coordinates = new List<GeoJson2DCoordinates>();

                                                        for (int m = 0; m < polygon.Points.Length - 1; m++)
                                                        {
                                                            Point point = polygon.Points[m];
                                                            GeoJson2DCoordinates geoJson2DCoordinates = new GeoJson2DCoordinates(point.X, point.Y);
                                                            coordinates.Add(geoJson2DCoordinates);
                                                        }

                                                        GeoJsonPolygon<GeoJson2DCoordinates> geoJsonPolygon = GeoJson.Polygon(coordinates.ToArray());

                                                        bsonDocument.Add(columnName, geoJsonPolygon.ToBsonDocument());
                                                    }
                                                }
                                            }
                                        }
                                    }
                                    collection.Insert(bsonDocument);
                                }
                            }
                        }
                    }
                }
            }
        }
    }
}
