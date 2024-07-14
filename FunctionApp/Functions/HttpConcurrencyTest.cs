using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Azure.WebJobs;
using Microsoft.Azure.WebJobs.Extensions.Http;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;
using ExecutionContext = Microsoft.Azure.WebJobs.ExecutionContext;

namespace FunctionApp.Functions;

public class HttpConcurrencyTest
{
    private readonly ILogger<HttpConcurrencyTest> _logger;

    public HttpConcurrencyTest(ILogger<HttpConcurrencyTest> logger, IConfiguration configuration)
    {
        _logger = logger;
    }

    [FunctionName(nameof(HttpConcurrencyTest))]
    public async Task<IActionResult> RunAsync(
        [HttpTrigger(AuthorizationLevel.Function, "get", "post", Route = null)] HttpRequest req, 
        [Queue("concurrency-test", Connection = "AzureWebJobsStorage")] IAsyncCollector<string> queue,
        ExecutionContext executionContext, CancellationToken cancellationToken)
    {
        req.Query.TryGetValue("count", out var countValues);
        int.TryParse(countValues.FirstOrDefault(), out var count);
        if (count <= 0) count = 32;

        for (var i = 1; i <= count; i++)
        {
            await queue.AddAsync(i.ToString(), CancellationToken.None);
        }
        
        return new OkResult();
    }
}