namespace ElasticUp.Tests.Sample
{
    public class SampleObject
    {
        public int Number { get; set; }
    }

    public class SampleObjectWithId
    {
        public ObjectId Id { get; set; }
        public int Number { get; set; }
    }

    public class ObjectId
    {
        public string Type { get; set; }
        public int Sequence { get; set; }
        public override string ToString()
        {
            return $"{Type}-{Sequence}";
        }
    }
}