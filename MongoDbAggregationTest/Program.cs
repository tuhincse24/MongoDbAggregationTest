using Microsoft.Extensions.Configuration;
using System;
using System.IO;
using System.Collections.Generic;

using MongoDB.Bson;
using MongoDB.Bson.IO;
namespace MongoDbAggregationTest
{
    class Program
    {
        static void Main(string[] args)
        {
            var builder = new ConfigurationBuilder()
            .SetBasePath(Directory.GetCurrentDirectory())
            .AddJsonFile("appsettings.json", optional: false, reloadOnChange: true)
            .AddEnvironmentVariables();

            IConfigurationRoot configuration = builder.Build();

            var mdbSettings = new MongoDBSettings();
            configuration.GetSection("MongoDB").Bind(mdbSettings);

            Console.WriteLine($"Cluster Connection Uri is '{mdbSettings.ConnectionUri}'");
            Console.WriteLine($"DB Database Name is '{mdbSettings.DatabaseName}'");
            Console.WriteLine($"DB Collection Name is '{mdbSettings.CollectionName}'");

            DbManager dbMgr = new DbManager(mdbSettings.ConnectionUri, mdbSettings.DatabaseName);
            var collection = dbMgr.GetCollection(mdbSettings.CollectionName);
            var colStates = dbMgr.GetStatesCollection(mdbSettings.CollectionName);

            int index = (args != null && args.Length > 0) ? Int32.Parse(args[0]) : 1;
            string strMode = (args != null && args.Length > 1) ? args[1] : "bson";
            Console.WriteLine($"Command line parameter is '{index}'");

            List<BsonDocument> resData = new List<BsonDocument>();
            string results = string.Empty;
            DateTime dtStart = DateTime.Now;

            if (strMode.ToLower() == "linq")
            {
                switch (index)
                {
                    case 1:
                        Console.WriteLine("Total US Area with average region area is:\r\n");
                        results = dbMgr.GetTotalUSArea(colStates);
                        break;
                    case 2:
                        Console.WriteLine("Area by US Census region (with states) are:\r\n");
                        List<CensusArea> censusAreas = dbMgr.GetAreaByRegion(colStates);
                        results = censusAreas.ToJson(new JsonWriterSettings { Indent = true });
                        break;
                    case 3:
                        Console.WriteLine("Total US population by census year:\r\n");
                        results = dbMgr.GetPopulationByYear(colStates);
                        break;
                    case 4:
                        Console.WriteLine("Southern States population by census year:\r\n");
                        results = dbMgr.GetSouthernStatesPopulationByYear(colStates);
                        break;
                    case 5:
                        Console.WriteLine("Population delta between 1990 and 2010 by state:\r\n");
                        results = dbMgr.GetPopulationDeltaByState(colStates);
                        break;
                    case 6:
                        Console.WriteLine("Population in states within 500 km of Memphis:\r\n");
                        resData = dbMgr.GetPopulationByState500KmsAroundMemphis(collection);
                        break;
                    case 7:
                        Console.WriteLine("Population in states within 500 km of Memphis (stored in database collection):\r\n");
                        resData = dbMgr.GetPopulationByState500KmsAroundMemphis(collection, "peopleNearMemphis");
                        break;
                    case 8:
                        Console.WriteLine("State population density comparison in 1990 and 2010 :\r\n");
                        results = dbMgr.GetPopulationDensityByRegion(colStates);
                        break;
                    default:
                        results = dbMgr.GetTotalUSArea(colStates);
                        break;
                }
            }
            else
            {
                switch (index)
                {
                    case 1:
                        Console.WriteLine("Total US Area with average region area is:\r\n");
                        resData = dbMgr.GetTotalUSArea(collection);
                        break;
                    case 2:
                        Console.WriteLine("Area by US Census region (with states) are:\r\n");
                        resData = dbMgr.GetAreaByRegion(collection);
                        break;
                    case 3:
                        Console.WriteLine("Total US population by census year:\r\n");
                        resData = dbMgr.GetPopulationByYear(collection);
                        break;
                    case 4:
                        Console.WriteLine("Southern States population by census year:\r\n");
                        resData = dbMgr.GetSouthernStatesPopulationByYear(collection);
                        break;
                    case 5:
                        Console.WriteLine("Population delta between 1990 and 2010 by state:\r\n");
                        resData = dbMgr.GetPopulationDeltaByState(collection);
                        break;
                    case 6:
                        Console.WriteLine("Population in states within 500 km of Memphis:\r\n");
                        resData = dbMgr.GetPopulationByState500KmsAroundMemphis(collection);
                        break;
                    case 7:
                        Console.WriteLine("Population in states within 500 km of Memphis (stored in database collection):\r\n");
                        resData = dbMgr.GetPopulationByState500KmsAroundMemphis(collection, "peopleNearMemphis");
                        break;
                    case 8:
                        Console.WriteLine("State population density comparison in 1990 and 2010 :\r\n");
                        resData = dbMgr.GetPopulationDensityByState(collection);
                        break;
                    default:
                        resData = dbMgr.GetTotalUSArea(collection);
                        break;
                }
            }

            DateTime dtEnd = DateTime.Now;
            if (resData.Count > 0)
            {
                results = resData.ToJson(new JsonWriterSettings { Indent = true });
            }
            Console.WriteLine(results);
            Console.WriteLine($"{strMode.ToUpperInvariant()} method took {(dtEnd - dtStart).TotalMilliseconds} ms");
            Console.WriteLine("Press Enter to exit");
            Console.ReadLine();
        }
    }
}
