using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using System.IO;
using Datafordelen.Address;
using Datafordelen.GeoData;
using Datafordelen.Config;
using Datafordelen.Ftp;
using Datafordelen.Kafka;

namespace Datafordelen.Internal
{
    public static class Setup
    {
        public static ServiceProvider Configure()
        {
            var builder = new ConfigurationBuilder();
            var configuration = SetupAppSettings(builder);

            var serviceProvider = new ServiceCollection();
            ConfigureServices(serviceProvider, configuration);

            return serviceProvider.BuildServiceProvider();
        }

        private static IConfigurationRoot SetupAppSettings(ConfigurationBuilder builder)
        {
            return builder
                .SetBasePath(Directory.GetCurrentDirectory())
                .AddJsonFile("appsettings.json", optional: true, reloadOnChange: true)
                .Build();
        }

        private static void ConfigureServices(ServiceCollection serviceCollection, IConfigurationRoot configuration)
        {
            serviceCollection.Configure<AppSettings>(configuration.GetSection("AppSettings"));
            serviceCollection.AddSingleton<Startup, Startup>();
            serviceCollection.AddSingleton<IGeoDataService, GeoDataService>();
            serviceCollection.AddSingleton<IAddressService, AddressService>();
            serviceCollection.AddSingleton<IFTPClient,FTPClient>();
            serviceCollection.AddSingleton<IKakfkaProducer,KafkaProducer>();
            serviceCollection.AddLogging();
        }
    }
}
