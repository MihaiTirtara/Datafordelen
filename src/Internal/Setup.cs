using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using System.IO;
using Datafordelen.Address;
using Datafordelen.GeoData;
using Datafordelen.Config;
using Datafordelen.Ftp;
using Datafordelen.Kafka;
using Microsoft.Extensions.Logging;

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
            serviceCollection.AddSingleton<IFTPClient, FTPClient>();
            serviceCollection.AddSingleton<IKafkaProducer, KafkaProducer>();
            serviceCollection.AddLogging();

            serviceCollection.AddLogging(configure => configure.AddConsole())
                    .AddTransient<AddressService>();
            serviceCollection.AddLogging(configure => configure.AddConsole())
                    .AddTransient<GeoDataService>();
            serviceCollection.AddLogging(configure => configure.AddConsole())
            .AddTransient<FTPClient>();
            serviceCollection.AddLogging(configure => configure.AddConsole())
            .AddTransient<KafkaProducer>();
        }
    }
}
