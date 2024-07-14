using System;
using System.Diagnostics;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using System.Threading.Tasks.Dataflow;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Azure.WebJobs;
using Microsoft.Azure.WebJobs.Extensions.Http;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;
using ExecutionContext = Microsoft.Azure.WebJobs.ExecutionContext;

namespace CCBA.Testing.ConcurrencyLimiterTest.Functions;

public class HttpConcurrencyTest 
{
    private readonly ILogger<HttpConcurrencyTest> _logger;
    private readonly Services.ConcurrencyLimiterService _concurrencyLimiterService;

    public HttpConcurrencyTest(ILogger<HttpConcurrencyTest> logger, IConfiguration configuration, ConcurrencyLimiterTest.Services.ConcurrencyLimiterService concurrencyLimiterService)
    {
        _logger = logger;
        _concurrencyLimiterService = concurrencyLimiterService;
    }

    [FunctionName(nameof(HttpConcurrencyTest))]
    public async Task<IActionResult> RunAsync([HttpTrigger(AuthorizationLevel.Function, "get", "post", Route = null)] HttpRequest req, ExecutionContext executionContext, CancellationToken cancellationToken)
    {
        const int maxDegreeOfParallelism = 2;
        
        var actionBlock = new ActionBlock<string>(async data =>
        {
            try
            {
                await SendSomeDataSomewhereWithConcurrencyLimit(data, maxDegreeOfParallelism, executionContext, CancellationToken.None); // we want to limit how many of these are executed concurrently in a distributes system
            }
            catch (Exception e)
            {
                _logger.LogError(e, e.Message);
            }

        }, new ExecutionDataflowBlockOptions
        {
            MaxDegreeOfParallelism = maxDegreeOfParallelism,
        });

        req.Query.TryGetValue("count", out var countValues);
        int.TryParse(countValues.FirstOrDefault(), out var count);
        if (count <= 0) count = 100;
        
        for (var i = 1; i <= count; i++) await actionBlock.SendAsync(i.ToString(), CancellationToken.None);

        actionBlock.Complete();

        return new OkResult();
    }
    
    private async Task SendSomeDataSomewhereWithConcurrencyLimit(string s, int maxDegreeOfParallelism, ExecutionContext executionContext, CancellationToken cancellationToken)
    {
        await using var _ = await _concurrencyLimiterService.WaitForConcurrentLeaseAsync("concurrency-test", maxDegreeOfParallelism, TimeSpan.Zero, executionContext, cancellationToken);
       
        _logger.LogInformation($"Sending {s}", LogLevel.Information);
        var stopwatch = Stopwatch.StartNew();
        await Task.Delay(TimeSpan.FromSeconds(1), cancellationToken); // simulate some work being done hat takes time
        _logger.LogInformation($"Sent {s} Elapsed={stopwatch.Elapsed}");
    }
}