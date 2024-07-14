using System;
using System.Diagnostics;
using System.Threading;
using System.Threading.Tasks;
using Azure.Messaging.ServiceBus;
using CCBA.Integrations.Base.Abstracts;
using CCBA.Integrations.Base.Helpers;
using Microsoft.Azure.WebJobs;
using Microsoft.Azure.WebJobs.Host;
using Microsoft.Azure.WebJobs.ServiceBus;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;
using ExecutionContext = Microsoft.Azure.WebJobs.ExecutionContext;

namespace CCBA.Testing.ConcurrencyLimiterTest.Functions;

public class ServiceBusConcurrencyTest :BaseLogger
{
    private readonly Services.ConcurrencyLimiterService _concurrencyLimiterService;

    public ServiceBusConcurrencyTest(ILogger<BaseLogger> logger, IConfiguration configuration, Services.ConcurrencyLimiterService concurrencyLimiterService) : base(logger, configuration)
    {
        _concurrencyLimiterService = concurrencyLimiterService;
    }

    [FunctionName("ServiceBusConcurrencyTestOutbound1")]
    public async Task RunAsync1([ServiceBusTrigger("_concurrencytest1", Connection = "ServiceBusConnection")] ServiceBusReceivedMessage message, 
        ServiceBusMessageActions messageActions, string lockToken, 
        ExecutionContext executionContext, CancellationToken cancellationToken)
    {
        var stopwatch = Stopwatch.StartNew();
        LogInformation($"Received message: {message.MessageId}");

        const int maxDegreeOfParallelism = 4;
        await using var _ = await _concurrencyLimiterService.WaitForConcurrentLeaseAsync("sb1", maxDegreeOfParallelism, TimeSpan.Zero, executionContext, cancellationToken);
        {
            await Task.Delay(TimeSpan.FromSeconds(0.5).AddJitter(1000), cancellationToken); // simulate some work being done that takes time
        }
        await messageActions.CompleteMessageAsync(message, cancellationToken);
        
        stopwatch.Stop();
        LogInformation($"Completed {message.MessageId}", LogLevel.Information, properties: new()
        {
            { "Elapsed", stopwatch.Elapsed.ToString() }
        });
    }
    
    [FunctionName("ServiceBusConcurrencyTestOutbound2")]
    public async Task RunAsync2([ServiceBusTrigger("_concurrencytest2", Connection = "ServiceBusConnection")] ServiceBusReceivedMessage message, 
        ServiceBusMessageActions messageActions, string lockToken, 
        ExecutionContext executionContext, CancellationToken cancellationToken)
    {
        var stopwatch = Stopwatch.StartNew();
        LogInformation($"Received message: {message.MessageId}");

        const int maxDegreeOfParallelism = 8;
        await using var _ = await _concurrencyLimiterService.WaitForConcurrentLeaseAsync("sb2", maxDegreeOfParallelism, TimeSpan.Zero, executionContext, cancellationToken);
        {
            await Task.Delay(TimeSpan.FromSeconds(0.5).AddJitter(1000), cancellationToken); // simulate some work being done that takes time
        }
        await messageActions.CompleteMessageAsync(message, cancellationToken);
        
        stopwatch.Stop();
        LogInformation($"Completed {message.MessageId}", LogLevel.Information, properties: new()
        {
            { "Elapsed", stopwatch.Elapsed.ToString() }
        });
    }
}