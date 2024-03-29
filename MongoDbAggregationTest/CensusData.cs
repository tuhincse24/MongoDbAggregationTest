﻿using MongoDB.Bson.Serialization.Attributes;

namespace MongoDbAggregationTest
{
    public class CensusData
    {
        [BsonElement("year")]
        public int Year { get; set; }

        [BsonElement("totalPop")]
        public int TotalPopulation { get; set; }

        [BsonElement("totalHouse")]
        public int TotalHouseholds { get; set; }

        [BsonElement("occHouse")]
        public int OccupiedHouseHolds { get; set; }
    }
}
