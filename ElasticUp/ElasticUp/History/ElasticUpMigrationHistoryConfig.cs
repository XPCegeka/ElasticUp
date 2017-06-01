using System;
using Nest;

namespace ElasticUp.History
{
    public class ElasticUpMigrationHistoryConfig
    {
        public static Func<MappingsDescriptor, IPromise<IMappings>> Mapping =>
            descriptor => descriptor
                .Map<ElasticUpMigrationHistory>(selector => selector
                    .AutoMap());
    }
}