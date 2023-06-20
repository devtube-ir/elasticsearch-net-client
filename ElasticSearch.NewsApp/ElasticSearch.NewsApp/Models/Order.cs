using Elastic.Clients.Elasticsearch;

namespace ElasticSearch.NewsApp.Models;

public class Order
{
   public string[] category { get; set; }
   public string currency { get; set; }
   public string customer_first_name { get; set; }
   public string customer_full_name { get; set; }
   public string customer_gender { get; set; }
   public int customer_id { get; set; }
   public string customer_last_name { get; set; }
   public string customer_phone { get; set; }
   public string day_of_week { get; set; }
   public int day_of_week_i { get; set; }
   public string email { get; set; }
   public string[] manufacturer { get; set; }
   public DateTime order_date { get; set; }
   public int order_id { get; set; }
   public Product[] products { get; set; }
   public string[] sku { get; set; }
   public float taxful_total_price { get; set; }
   public float taxless_total_price { get; set; }
   public int total_quantity { get; set; }
   public int total_unique_products { get; set; }
   public string type { get; set; }
   public string user { get; set; }
   public Geoip geoip { get; set; }
   public Event _event { get; set; }
}



public class Geoip
{
   public string country_iso_code { get; set; }
   public Location location { get; set; }
   public string region_name { get; set; }
   public string continent_name { get; set; }
   public string city_name { get; set; }
}

public class Location
{
   public float lon { get; set; }
   public float lat { get; set; }
}

public class Event
{
   public string dataset { get; set; }
}

public class Product
{
   public float base_price { get; set; }
   public int discount_percentage { get; set; }
   public int quantity { get; set; }
   public string manufacturer { get; set; }
   public int tax_amount { get; set; }
   public int product_id { get; set; }
   public string category { get; set; }
   public string sku { get; set; }
   public float taxless_price { get; set; }
   public int unit_discount_amount { get; set; }
   public float min_price { get; set; }
   public string _id { get; set; }
   public int discount_amount { get; set; }
   public DateTime created_on { get; set; }
   public string product_name { get; set; }
   public float price { get; set; }
   public float taxful_price { get; set; }
   public float base_unit_price { get; set; }
}
