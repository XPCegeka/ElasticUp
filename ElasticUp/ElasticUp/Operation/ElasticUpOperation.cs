using ElasticUp.Migration.Meta;

namespace ElasticUp.Operation
{
    public class ElasticUpOperation
    {
        public int OperationNumber { get; private set; }
        internal VersionedIndexName FromIndex { get; set; }
        internal VersionedIndexName ToIndex { get; set; }

        public ElasticUpOperation(int operationNumber)
        {
            OperationNumber = operationNumber;
        }

        public void Execute()
        {
        }

        internal ElasticUpOperation From(VersionedIndexName indexName)
        {
            FromIndex = indexName;

            return this;
        }

        internal ElasticUpOperation To(VersionedIndexName indexName)
        {
            ToIndex = indexName;

            return this;
        }
    }
}