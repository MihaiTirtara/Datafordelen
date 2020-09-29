using System.IO;
using System.Threading.Tasks;
using Microsoft.Extensions.Configuration;
using Datafordelen.Config;
using Datafordelen.GeoData;
using Datafordelen.Address;

namespace Datafordelen
{
    class Program
    {
        private static FTPClient client = new FTPClient();

        public static async Task Main(string[] args)
        {
            var builder = new ConfigurationBuilder()
                .SetBasePath(Directory.GetCurrentDirectory())
                .AddJsonFile("appsettings.json", optional: true, reloadOnChange: true);

            var appSettings = new AppSettings();

            var configuration = builder.Build();
            ConfigurationBinder.Bind(configuration.GetSection("appSettings"), appSettings);

            var addressService = new AddressService(appSettings);
            var geoDataService = new GeoDataService(appSettings);

            await addressService.GetinitialAddressData();
        }
    }
}
