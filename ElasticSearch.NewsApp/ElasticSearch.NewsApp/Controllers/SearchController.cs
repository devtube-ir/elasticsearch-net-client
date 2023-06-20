using Dapper;
using Elastic.Clients.Elasticsearch;
using Elastic.Clients.Elasticsearch.Aggregations;
using Elastic.Clients.Elasticsearch.Core.Search;
using Elastic.Clients.Elasticsearch.IndexManagement;
using Elastic.Clients.Elasticsearch.QueryDsl;
using ElasticSearch.NewsApp.Models;
using Microsoft.AspNetCore.Mvc;
using System.Data;
using System.Data.SqlClient;
using System.Globalization;
using System.Linq.Expressions;
using static System.Net.Mime.MediaTypeNames;

namespace ElasticSearch.NewsApp.Controllers
{
   [ApiController]
   [Route("[controller]")]
   public class SearchController : ControllerBase
   {
      private readonly ILogger<SearchController> _logger;

      public SearchController(ILogger<SearchController> logger)
      {
         _logger = logger;
      }

      [HttpGet("/search-sql")]
      public async Task<IActionResult> GetAsync(string? searchTitle)
      {
         var connection_string = "Server=.;Database=ElasticSearchTutorial;Integrated Security=true;MultipleActiveResultSets=True;";
         using var connection = new SqlConnection(connection_string);

         var query = "SELECT TOP 100 * FROM [News] WHERE [Title] LIKE CONCAT(N'%',@Title,'%')";

         var dbparams = new DynamicParameters();
         dbparams.Add("Title", searchTitle, DbType.String);

         var items = await connection.QueryAsync<News>(query, dbparams, commandType: CommandType.Text);

         return Ok(items);
      }

      private ElasticsearchClient GetClient()
      {
         var settings = new ElasticsearchClientSettings(
          new Uri("http://localhost:9200")
         );

         var client = new ElasticsearchClient(settings);

         return client;
      }

      private const string INDEX_NAME = "news-index-2";

      [HttpGet("/health")]
      public async Task<IActionResult> GetElasticHealthAsync()
      {
         var client = GetClient();

         var health = await client.Cluster.HealthAsync();

         return Ok(health.Status);
      }

      [HttpPost("/news")]
      public async Task<IActionResult> IndexNewsAsync(News news)
      {
         var client = GetClient();

         var response = await client.IndexAsync(news, INDEX_NAME);

         if (!response.IsValidResponse)
            throw new Exception("Could not index document");

         return Ok($"Index document with ID {response.Id} succeeded.");
      }

      [HttpGet("/news/{id}")]
      public async Task<IActionResult> GetIndexedDocument(long id)
      {
         var client = GetClient();

         var response = await client.GetAsync<News>(id, idx => idx.Index(INDEX_NAME));

         if (!response.IsValidResponse)
            throw new Exception("Could not get document");

         return Ok(response.Source);
      }

      [HttpPut("/news/{id}")]
      public async Task<IActionResult> UpdateAsync(long id, News news)
      {
         var client = GetClient();

         var response = await client.UpdateAsync<News, News>(INDEX_NAME, id, u => u.Doc(news));

         if (!response.IsValidResponse)
            throw new Exception("Could not get document");

         return Ok("Updated");
      }

      [HttpDelete("/news/{id}")]
      public async Task<IActionResult> DeleteAsync(long id)
      {
         var client = GetClient();

         var response = await client.DeleteAsync(INDEX_NAME, id);

         if (!response.IsValidResponse)
            throw new Exception("Could not delete document");

         return Ok("Deleted");
      }

      [HttpGet("/news")]
      public async Task<IActionResult> SearchAsync(string? title)
      {
         var client = GetClient();

         var response = await client.SearchAsync<News>(s => s
            .Index(INDEX_NAME)
            .From(0)
            .Size(10)
            .Query(q => q
               .Match(m => m
                  .Field(f => f.Title)
                  .Query(title ?? "")
                  .Fuzziness(new Fuzziness("AUTO"))
               )
            )
         );

         if (!response.IsValidResponse)
            throw new Exception("Could not search document");

         return Ok(response.Documents);
      }

      [HttpPost("/create/index")]
      public async Task<IActionResult> MapAsync()
      {
         var indexName = INDEX_NAME;

         var client = GetClient();

         var response = await client.Indices.CreateAsync(indexName, i => i
            .Mappings(m => m.
               Properties<News>(p => p
                  .Keyword(t => t.Id)
                  .Text(t => t.Title)
               )
            )
         );

         if (!response.IsValidResponse)
            throw new Exception("Could not create index");

         return Ok($"Index creation with name {indexName} succeeded.");
      }

      [HttpPost("/news/batch")]
      public async Task<IActionResult> CreateBatchAsync()
      {
         var client = GetClient();

         var connection_string = "Server=.;Database=ElasticSearchTutorial;Integrated Security=true;MultipleActiveResultSets=True;";

         var chunkSize = 5_000;
         var totalCount = 2_104_859;

         using var connection = new SqlConnection(connection_string);

         for (int step = 0; step < totalCount / chunkSize; step++)
         {
            var query = $"SELECT * FROM [News] ORDER BY [Id] OFFSET {chunkSize * step} ROWS FETCH NEXT {chunkSize} ROWS ONLY";

            var items = await connection.QueryAsync<News>(query, commandType: CommandType.Text);

            var response = await client.IndexManyAsync(items, INDEX_NAME);

            if (!response.IsValidResponse)
               throw new Exception($"Could not index @ step:{step}");
         }

         return Ok($"DONE.");
      }

      private const string ECOMMERCE_INDEX_NAME = "kibana_sample_data_ecommerce";

      [HttpGet("/e-commerce")]
      public async Task<IActionResult> SearchEcommerceAsync()
      {
         var users = new string[] { "kimchy" };

         var client = GetClient();

         var response = await client.SearchAsync<Order>(s => s
            .Index(ECOMMERCE_INDEX_NAME)
            .From(0)
            .Size(10)
            .Query(q => q
               .Bool(b => b
                  .Should(
                     s => s.Term(t => t.user, "kimchy"),
                     s => s.Terms(t => t
                        .Field(f => f.user)
                        .Terms(new TermsQueryField(users.Select(user => FieldValue.String(user)).ToArray()))),
                     s => s.Range(r => r.DateRange(r => r.Field(f => f.order_date).Lt(DateTime.Now))),
                     s => s.Exists(e => e.Field(f => f.user)),
                     s => s.Ids(i => i.Values(new string[] { "1", "2" }))
                  )
               )
            )
            .Source(new SourceConfig(filter: new SourceFilter() { Includes = new string[] { "user" } }))
            .Aggregations(a => a
               .Cardinality("unique-skus", c => c.Field(f => f.sku))
               .Avg("average-price", av => av.Field(f => f.products[0].price))
               .Stats("quantity-stats", s => s.Field(f => f.total_quantity))
             .Terms("product-name", s => s.Field("products.product_name.keyword"))
            )
         );

         if (!response.IsValidResponse)
            throw new Exception("Could not search document");

         var aggregation = response.Aggregations;

         return Ok(new
         {
            response.Documents,
            response.HitsMetadata.Total,
         });
      }

   }
}