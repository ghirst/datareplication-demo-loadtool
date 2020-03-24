using Amazon.S3;
using Gentrack.Tools.DataReplicationLoadTool.Consumers;
using Gentrack.Tools.DataReplicationLoadTool.Producers;
using Gentrack.Tools.DataReplicationLoadTool.Providers;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;

namespace Gentrack.Tools.DataReplicationLoadTool
{
    internal class Startup
    {
        public IConfigurationRoot Configuration { get; }

        public Startup()
        {
            var builder = new ConfigurationBuilder()
                .AddJsonFile("appsettings.json");

            Configuration = builder.Build();
        }

        public void ConfigureServices(IServiceCollection services)
        {
            services.AddLogging(configure =>
            {
                configure.AddConfiguration(Configuration.GetSection("Logging"));
                configure.AddConsole();
            }).Configure<LoggerFilterOptions>(options => options.MinLevel = LogLevel.Information);

            services.AddSingleton<IConfigurationRoot>(Configuration);

            // Add AWS Services
            services.AddDefaultAWSOptions(Configuration.GetAWSOptions());
            services.AddAWSService<IAmazonS3>();

            // Add Our Own Services
            services.AddTransient<IDatabaseService, SqlServerDatabaseService>();
            services.AddTransient<IRemoteEndpointService, S3Service>();
            services.AddTransient<IFileProducer, S3FileProducer>();
            services.AddTransient<IFullLoadFileConsumer, FullLoadFileConsumer>();
            services.AddTransient<IDeltaFileConsumer, DeltaFileConsumer>();
            services.AddTransient<ILocalCacheService, LocalCacheService>();
        }
    }
}