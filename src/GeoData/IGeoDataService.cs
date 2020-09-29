using System.Threading.Tasks;

namespace Datafordelen.GeoData
{
    public interface IGeoDataService
    {
        public Task GetLatestGeoData();
    }
}
