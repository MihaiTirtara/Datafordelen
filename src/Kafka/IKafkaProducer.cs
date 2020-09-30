using System.Collections.Generic;

namespace Datafordelen.Kafka
{
     public interface IKakfkaProducer
     {
        void Produce(string topicname, List<string> batch);

     }
}