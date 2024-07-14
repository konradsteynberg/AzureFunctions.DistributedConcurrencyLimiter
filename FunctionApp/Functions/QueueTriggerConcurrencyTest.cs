using System.Threading.Tasks;
using System;
using System.Threading;
using FunctionApp.Services;
using Microsoft.Azure.WebJobs;
using Microsoft.Extensions.Logging;
using ExecutionContext = Microsoft.Azure.WebJobs.ExecutionContext;

namespace FunctionApp.Functions;

public class QueueTriggerConcurrencyTest
{
    private readonly ILogger<QueueTriggerConcurrencyTest> _logger;
    private readonly ConcurrencyLimiterService _concurrencyLimiterService;

    private static readonly Random _random = new();
    
    public QueueTriggerConcurrencyTest(ILogger<QueueTriggerConcurrencyTest> logger, ConcurrencyLimiterService concurrencyLimiterService)
    {
        _logger = logger;
        _concurrencyLimiterService = concurrencyLimiterService;
    }
    
    [FunctionName(nameof(QueueTriggerConcurrencyTest))]
    public async Task RunAsync(
        [QueueTrigger("concurrency-test", Connection = "AzureWebJobsStorage")] string myQueueItem,
        ExecutionContext executionContext, CancellationToken cancellationToken)
    {
        await using var _ = await _concurrencyLimiterService.WaitForConcurrentLeaseAsync("concurrency-test", 4, TimeSpan.Zero, executionContext, cancellationToken);
        await Task.Delay(_random.Next(4000, 5000), cancellationToken);
        _logger.LogInformation($"C# Queue trigger function processed: {myQueueItem}");
    }
}