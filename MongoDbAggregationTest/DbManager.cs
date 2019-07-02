using System;
using System.Collections.Generic;
using System.Linq;

using MongoDB.Bson;
using MongoDB.Bson.IO;
using MongoDB.Driver;
using MongoDB.Driver.Linq;

namespace MongoDB.Samples.AggregationFramework.Library
{
    public class DbManager
    {
        private volatile IMongoCollection<BsonDocument> m_collection;
        private volatile IMongoCollection<State> m_colStates;
        private volatile IMongoClient m_client;
        private static object syncRoot = new Object();
        private string m_ConnectionUri;
        private string m_DatabaseName;

        private DbManager() { }

        public DbManager(string strConnectionUri, string strDatabaseName)
        {
            m_ConnectionUri = strConnectionUri;
            m_DatabaseName = strDatabaseName;
        }

        public IMongoCollection<BsonDocument> GetCollection(string collectionName)
        {
            if (m_collection == null)
            {
                lock (syncRoot)
                {
                    if (m_client == null)
                    {
                        m_client = new MongoClient(m_ConnectionUri);
                    }
                    if (m_collection == null)
                    {
                        var db = m_client.GetDatabase(m_DatabaseName);
                        m_collection = db.GetCollection<BsonDocument>(collectionName);
                    }
                }
            }
            return m_collection;
        }

        public IMongoCollection<State> GetStatesCollection(string collectionName)
        {
            if (m_colStates == null)
            {
                lock (syncRoot)
                {
                    if (m_client == null)
                    {
                        m_client = new MongoClient(m_ConnectionUri);
                    }
                    if (m_colStates == null)
                    {
                        var db = m_client.GetDatabase(m_DatabaseName);
                        m_colStates = db.GetCollection<State>(collectionName);
                    }
                }
            }
            return m_colStates;
        }

        public List<BsonDocument> GetTotalUSArea(IMongoCollection<BsonDocument> collection)
        {
            var aggregate = collection.Aggregate().Group(new BsonDocument {
                { "_id", BsonNull.Value },
                { "totalArea", new BsonDocument("$sum", "$areaM") },
                { "avgArea", new BsonDocument("$avg", "$areaM") }
            });
            return aggregate.ToList();
        }

        public string GetTotalUSArea(IMongoCollection<State> collection)
        {
            var aggregate = collection.AsQueryable()
                .GroupBy(p => p.Region, (k, s) => new
                {
                    totalArea = s.Sum(y => y.AreaSquareMiles),
                    avgArea = s.Average(y => y.AreaSquareMiles),
                });
            return aggregate.ToList().ToJson(new JsonWriterSettings { Indent = true });
        }

        public List<BsonDocument> GetAreaByRegion(IMongoCollection<BsonDocument> collection)
        {
            var aggregate = collection.Aggregate()
                .Group(new BsonDocument
                {
                    { "_id", "$region" },
                    { "totalArea", new BsonDocument("$sum", "$areaM") },
                    { "avgArea", new BsonDocument("$avg", "$areaM") },
                    { "numStates", new BsonDocument("$sum", 1) },
                    { "states", new BsonDocument("$push", "$name") }
                });
            return aggregate.ToList();
        }

        public List<CensusArea> GetAreaByRegion(IMongoCollection<State> collection)
        {
            var aggregate = collection.AsQueryable()
                .GroupBy(p => p.Region, (k, s) => new CensusArea
                {
                    Id = k,
                    TotalArea = s.Sum(y => y.AreaSquareMiles),
                    AverageArea = s.Average(y => y.AreaSquareMiles),
                    StatesCount = s.Count(),
                    States = s.Select(y => y.Name)
                });
            return aggregate.ToList();
        }

        public List<BsonDocument> GetPopulationByYear(IMongoCollection<BsonDocument> collection)
        {
            var aggregate = collection.Aggregate()
                .Unwind("data")
                .Group(new BsonDocument
                {
                    { "_id", "$data.year" },
                    { "totalPop", new BsonDocument("$sum", "$data.totalPop") }
                })
                .Sort(new BsonDocument("totalPop", 1));
            return aggregate.ToList();
        }

        public string GetPopulationByYear(IMongoCollection<State> collection)
        {
            var aggregate = collection.AsQueryable()
                .SelectMany(s => s.Data)
                .GroupBy(d => d.Year)
                .Select(d => new { Id = d.Key, totalPop = d.Sum(y => y.TotalPopulation) })
                .OrderBy(p => p.totalPop);
            return aggregate.ToList().ToJson(new JsonWriterSettings { Indent = true });
        }

        public List<BsonDocument> GetSouthernStatesPopulationByYear(IMongoCollection<BsonDocument> collection)
        {
            var aggregate = collection.Aggregate()
                .Match(new BsonDocument("region", "South"))
                .Unwind("data")
                .Group(new BsonDocument
                {
                    { "_id", "$data.year" },
                    { "totalPop", new BsonDocument("$sum", "$data.totalPop") }
                })
                .Sort(new BsonDocument("totalPop", 1))
            ;
            return aggregate.ToList();
        }

        public string GetSouthernStatesPopulationByYear(IMongoCollection<State> collection)
        {
            var aggregate = collection.AsQueryable()
                .Where(s => s.Region == "South")
                .SelectMany(s => s.Data)
                .GroupBy(d => d.Year)
                .Select(d => new { Id = d.Key, totalPop = d.Sum(y => y.TotalPopulation) })
                .OrderBy(p => p.totalPop);
            return aggregate.ToList().ToJson(new JsonWriterSettings { Indent = true });
        }

        public List<BsonDocument> GetPopulationDeltaByState(IMongoCollection<BsonDocument> collection)
        {
            var aggregate = collection.Aggregate()
                .Unwind("data")
                .Sort(new BsonDocument("data.year", 1))
                .Group(new BsonDocument
                {
                    { "_id", "$name" },
                    { "pop1990", new BsonDocument("$first", "$data.totalPop") },
                    { "pop2010", new BsonDocument("$last", "$data.totalPop") }
                })
                .Project(new BsonDocument
                {
                    { "_id", 0 },
                    { "name", "$_id" } ,
                    {  "delta", new BsonDocument("$subtract", new BsonArray() {"$pop2010", "$pop1990"}) },
                    {  "deltaPercent", new BsonDocument(
                        "$trunc", new BsonDocument(
                            "$multiply", new BsonArray() {new BsonDocument(
                                "$subtract", new BsonArray () { new BsonDocument(
                                    "$divide", new BsonArray() { "$pop2010", "$pop1990" }),
                                    1 }),
                                100 })
                            )},
                    { "pop1990", 1 },
                    { "pop2010", 1 }
                }
                )
                .Sort(new BsonDocument("deltaPercent", 1))
            ;
            return aggregate.ToList();
        }

        public string GetPopulationDeltaByState(IMongoCollection<State> collection)
        {
            var aggregate = collection.AsQueryable()
                .SelectMany(s => s.Data, (s, state) => new
                {
                    Name = s.Name,
                    Year = state.Year,
                    TotalPopulation = state.TotalPopulation
                })
                .OrderBy(d => d.Year)
                .GroupBy(d => d.Name)
                .Select(d => new
                {
                    Id = d.Key,
                    pop1990 = d.First().TotalPopulation,
                    pop2010 = d.Last().TotalPopulation,
                    delta = d.Last().TotalPopulation - d.First().TotalPopulation,
                    deltaPercent = Math.Truncate((double)100 * (d.Last().TotalPopulation / d.First().TotalPopulation - 1))
                })
                .OrderBy(d => d.deltaPercent)
                ;
            return aggregate.ToList().ToJson(new JsonWriterSettings { Indent = true });
        }

        /// <summary>
        /// Get total population by year in states the center of which is within a circle of 500 km radius around Memphis
        /// </summary>
        /// <param name="collection"></param>
        /// <returns></returns>
        public List<BsonDocument> GetPopulationByState500KmsAroundMemphis(IMongoCollection<BsonDocument> collection, string outCollection = "")
        {
            BsonDocument geoPoint = new BsonDocument
        {
            {"type","Point"},
            {"coordinates",new BsonArray(new Double[]{90, 35})}
        };
            var geoNearOptions = new BsonDocument {
                                {"near", geoPoint },
                                {"distanceField","dist.calculated"},
                                {"maxDistance", 500000 },
                                {"includeLocs",  "dist.location"},
                                {"spherical", true},
                            };
            //var geonear = new BsonDocument { { "$geoNear", geoNearOptions } };
            var stage = new BsonDocumentPipelineStageDefinition<BsonDocument, BsonDocument>(new BsonDocument { { "$geoNear", geoNearOptions } });
            var aggregate = collection.Aggregate()
                .AppendStage(stage)
                .Unwind("data")
                .Group(new BsonDocument
                {
                    { "_id", "$data.year" },
                    { "totalPop", new BsonDocument("$sum", "$data.totalPop") },
                    { "states",  new BsonDocument("$addToSet", "$name")}
                })
                .Sort(new BsonDocument("_id", 1));

            if (!string.IsNullOrWhiteSpace(outCollection))
            {
                aggregate.Out(outCollection);
            }
            return aggregate.ToList();
        }

        public List<BsonDocument> GetPopulationDensityByState(IMongoCollection<BsonDocument> collection)
        {
            var aggregate = collection.Aggregate()
                .Match(new BsonDocument("data.totalPop", new BsonDocument("$gt", 1000000)))
                .Unwind("data")
                .Sort(new BsonDocument("data.year", 1))
                .Group(new BsonDocument
                {
                    { "_id", "$name" },
                    { "pop1990", new BsonDocument("$first", "$data.totalPop") },
                    { "pop2010", new BsonDocument("$last", "$data.totalPop") },
                    { "areaM" , new BsonDocument("$last", "$areaM") },
                    { "division" , new BsonDocument("$last", "$division") }
                })
            .Group(new BsonDocument
            {
                { "_id", "$division" },
                { "_totalPop1990", new BsonDocument("$sum", "$pop1990") },
                { "_totalPop2010", new BsonDocument("$sum", "$pop2010") },
                { "_totalAreaM", new BsonDocument("$sum", "$areaM") },
            })
            .Match(new BsonDocument("_totalAreaM", new BsonDocument("$gt", 100000)))
            .Project(new BsonDocument
            {
                { "_id", 0 },
                { "division", "$_id" } ,
                { "density1990", new BsonDocument("$divide", new BsonArray() {"$_totalPop1990", "$_totalAreaM"}) },
                { "density2010", new BsonDocument("$divide", new BsonArray() {"$_totalPop2010", "$_totalAreaM"}) },
                { "densityDelta", new BsonDocument(
                        "$subtract", new BsonArray () {
                            new BsonDocument(
                            "$divide", new BsonArray() { "$_totalPop2010", "$_totalAreaM" }),
                            new BsonDocument(
                            "$divide", new BsonArray() { "$_totalPop1990", "$_totalAreaM" })
                        })
                },
                { "totalAreaM", "$_totalAreaM" },
                { "totalPop1990", "$_totalPop1990" },
                { "totalPop2010", "$_totalPop2010" },
            }
            )
            .Sort(new BsonDocument("densityDelta", -1))
            ;
            return aggregate.ToList();
        }

        public string GetPopulationDensityByRegion(IMongoCollection<State> collection)
        {
            var aggregate = collection.AsQueryable()
                .SelectMany(s => s.Data, (state, censusData) => new
                {
                    Name = state.Name,
                    Year = censusData.Year,
                    TotalPopulation = censusData.TotalPopulation,
                    AreaSquareMiles = state.AreaSquareMiles,
                    Division = state.Division
                })
                .Where(d => d.TotalPopulation > 1000000)
                .OrderBy(d => d.Year)
                .GroupBy(d => d.Name)
                .Select(d => new
                {
                    Id = d.Key,
                    pop1990 = d.First().TotalPopulation,
                    pop2010 = d.Last().TotalPopulation,
                    AreaSquareMiles = d.Last().AreaSquareMiles,
                    Division = d.Last().Division
                })
                .GroupBy(d => d.Division)
                .Select(d => new
                {
                    Id = d.Key,
                    totalPop1990 = d.Sum(y => y.pop1990),
                    totalPop2010 = d.Sum(y => y.pop2010),
                    totalAreaM = d.Sum(y => y.AreaSquareMiles),
                })
                .Where(d => d.totalAreaM > 100000)
                .Select(d => new
                {
                    division = d.Id,
                    totalPop1990 = d.totalPop1990,
                    totalPop2010 = d.totalPop2010,
                    totalAreaM = d.totalAreaM,
                    density1990 = d.totalPop1990 / d.totalAreaM,
                    density2010 = d.totalPop2010 / d.totalAreaM,
                    densityDelta = (d.totalPop2010 / d.totalAreaM) - (d.totalPop1990 / d.totalAreaM),
                })
                .OrderByDescending(d => d.densityDelta)
                ;
            return aggregate.ToList().ToJson(new JsonWriterSettings { Indent = true });
        }
    }
}
