namespace ElasticSearch.NewsApp.Models
{
   public class News
   {
      public long Id { get; set; }
      public string Title { get; set; } = default!;
      public string ShortLink { get; set; } = default!;
      public string Time { get; set; } = default!;
      public string Category { get; set; } = default!;
      public string NewsId { get; set; } = default!;
      public string Body { get; set; } = default!;
   }
}
