using System;
using System.Collections.Generic;
using System.Text;

namespace MongoDB.Samples.AggregationFramework.Library
{

    public class MongoDBSettings
    {
        public string ConnectionUri { get; set; }

        public string DatabaseName { get; set; }

        public string CollectionName { get; set; }
    }
}
