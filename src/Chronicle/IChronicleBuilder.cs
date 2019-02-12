using System;
using Microsoft.Extensions.DependencyInjection;

namespace Chronicle
{
    public interface IChronicleBuilder
    {
        IServiceCollection Services { get; }
        IChronicleBuilder UseInMemoryPersistence();
        IChronicleBuilder UseSagaLog<TSagaLog>() where TSagaLog : ISagaLog;
        IChronicleBuilder UseSagaDataRepository<TRepository>() where TRepository : ISagaStateRepository;
    }
}
