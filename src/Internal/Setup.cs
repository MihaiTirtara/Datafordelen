using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using System.IO;
using Datafordelen.Address;
using Datafordelen.GeoData;
using Datafordelen.Config;

namespace Datafordelen.Internal
{
    public static class Setup
    {
        public static ServiceProvider Configure()
        {
            var builder = new ConfigurationBuilder();
            SetupAppSettings(builder);
            var configuration = builder.Build();

            var serviceProvider = new ServiceCollection();
            ConfigureServices(serviceProvider, configuration);

            return serviceProvider.BuildServiceProvider();
        }

        private static void SetupAppSettings(ConfigurationBuilder builder)
        {
            builder
                .SetBasePath(Directory.GetCurrentDirectory())
                .AddJsonFile("appsettings.json", optional: true, reloadOnChange: true);
        }

        private static void ConfigureServices(ServiceCollection serviceCollection, IConfigurationRoot configuration)
        {
            serviceCollection.Configure<AppSettings>(configuration.GetSection("appSettings"));
            serviceCollection.AddSingleton<Startup, Startup>();
            serviceCollection.AddSingleton<IGeoDataService, GeoDataService>();
            serviceCollection.AddSingleton<IAddressService, AddressService>();
            serviceCollection.AddLogging();
        }
    }
}
