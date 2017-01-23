using Nest;

namespace ElasticUp.Operation.Reindex
{
    public class TransformedDocument<TSourceType, TDestinationType> where TSourceType : class
    {
        public IHit<TSourceType> Hit { get; set; }
        public TDestinationType TransformedDocment { get; set; }
    }
}