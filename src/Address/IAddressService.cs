using System.Threading.Tasks;

namespace Datafordelen.Address
{
    public interface IAddressService
    {
        public Task GetinitialAddressData();
        public Task GetLatestAddressData();
    }
}
