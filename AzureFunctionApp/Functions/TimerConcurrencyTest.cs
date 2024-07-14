using System;
using System.Diagnostics;
using System.Threading;
using System.Threading.Tasks;
using System.Threading.Tasks.Dataflow;
using CCBA.Integrations.Base.Abstracts;
using CCBA.Integrations.Base.Helpers;
using Microsoft.Azure.WebJobs;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;
using ExecutionContext = Microsoft.Azure.WebJobs.ExecutionContext;

namespace CCBA.Testing.ConcurrencyLimiterService.Functions;

public class TimerConcurrencyTest : BaseLogger
{
    private readonly ConcurrencyLimiterTest.Services.ConcurrencyLimiterService _concurrencyLimiterService;

    public TimerConcurrencyTest(ILogger<TimerConcurrencyTest> logger, IConfiguration configuration, ConcurrencyLimiterTest.Services.ConcurrencyLimiterService concurrencyLimiterService) : base(logger, configuration)
    {
        _concurrencyLimiterService = concurrencyLimiterService;
    }
    
    [Disable]
    [FunctionName(nameof(TimerConcurrencyTest))]
    public async Task RunAsync([TimerTrigger("0 0 */5 * * *", RunOnStartup = true)] TimerInfo myTimer, ExecutionContext executionContext, CancellationToken cancellationToken)
    {
        // generate some test data that requires 'sending'
        //const int maxDegreeOfParallelism = 4;

        var actionBlock = new ActionBlock<string>(async s =>
        {
            try
            {
                await SendSomeDataSomewhereWithConcurrencyLimit(s, executionContext, cancellationToken); // we want to limit how many of these are executed concurrently in a distributes system
            }
            catch (Exception e)
            {
                LogException(e);
            }

        }, new ExecutionDataflowBlockOptions
        {
            //MaxDegreeOfParallelism = maxDegreeOfParallelism, // simulates concurrency control on a queue similar to maxConcurrentCalls in host.json
            MaxDegreeOfParallelism = -1,
            //BoundedCapacity = maxDegreeOfParallelism,
            CancellationToken = cancellationToken
        });

        for (var i = 0; i < 10000; i++) await actionBlock.SendAsync(i.ToString(), cancellationToken);

        //Parallel.For(0, 100000, i => { SendSomeDataSomewhereWithConcurrencyLimit(i.ToString(), executionContext, cancellationToken).GetAwaiter().GetResult(); });
        
        actionBlock.Complete();
        await actionBlock.Completion;

        LogInformation("Completed");
    }

    private async Task SendSomeDataSomewhereWithConcurrencyLimit(string s, ExecutionContext executionContext, CancellationToken cancellationToken)
    {
        // even though the host.json limit of concurrent messages to process from ServiceBus for example might be 16,
        // we want a way to lower that limit artificially across all instances that might be running

        await using (var _ = await _concurrencyLimiterService.WaitForConcurrentLeaseAsync("test", 32, TimeSpan.Zero, executionContext, cancellationToken))
        {
            try
            {
                LogInformation($"Sending {s}", LogLevel.Information);
                var stopwatch = Stopwatch.StartNew();
                await Task.Delay(TimeSpan.FromSeconds(0.1).AddJitter(5000), cancellationToken); // simulate some work being done hat takes time
                LogInformation($"Sent {s}", LogLevel.Information, properties: new()
                {
                    { "Elapsed", stopwatch.Elapsed.ToString() }
                });

                // complete message here
            }
            catch (Exception e)
            {
                LogException(e);
                // dead-letter message here
            }
        } // when the using statement is exited, the lease is released and free for the next message to acquire
    }
}