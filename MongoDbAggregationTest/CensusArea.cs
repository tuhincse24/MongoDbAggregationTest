using System.Collections.Generic;
using MongoDB.Bson.Serialization.Attributes;

namespace MongoDB.Samples.AggregationFramework.Library
{
    public class CensusArea
    {
        public string Id { get; set; }

        [BsonElement("totalArea")]
        public double TotalArea { get; set; }

        [BsonElement("avgArea")]
        public double AverageArea { get; set; }

        [BsonElement("numStates")]
        public int StatesCount { get; set; }

        [BsonElement("states")]
        public IEnumerable<string> States { get; set; }
    }
}
