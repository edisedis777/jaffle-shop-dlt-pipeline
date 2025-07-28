# pipeline.py - Complete Jaffle Shop dlt Pipeline with Performance Optimizations
import dlt
from dlt.sources.helpers.rest_client import RESTClient
from dlt.sources.helpers.rest_client.paginators import JSONResponsePaginator
import time
import os
from typing import Iterator, Dict, Any

# Performance configuration
os.environ["EXTRACT__WORKERS"] = "4"
os.environ["NORMALIZE__WORKERS"] = "4"  
os.environ["LOAD__WORKERS"] = "4"
os.environ["DATA_WRITER__BUFFER_MAX_ITEMS"] = "10000"
os.environ["DATA_WRITER__FILE_MAX_ITEMS"] = "50000"
os.environ["DATA_WRITER__DISABLE_COMPRESSION"] = "False"

# NAIVE VERSION (for comparison)
def jaffle_shop_source_naive():
    """Naive implementation - single threaded, row by row"""
    
    @dlt.resource(name="customers", write_disposition="replace")
    def get_customers():
        client = RESTClient(base_url="https://jaffle-shop.scalevector.ai/api/v1")
        
        page = 1
        while True:
            response = client.get("/customers", params={"page": page, "limit": 100})
            data = response.json()
            
            if not data.get("data"):
                break
                
            # Yield single rows (inefficient)
            for item in data["data"]:
                yield item
            
            page += 1
            if page > data.get("total_pages", 1):
                break
    
    @dlt.resource(name="orders", write_disposition="replace")  
    def get_orders():
        client = RESTClient(base_url="https://jaffle-shop.scalevector.ai/api/v1")
        
        page = 1
        while True:
            response = client.get("/orders", params={"page": page, "limit": 100})
            data = response.json()
            
            if not data.get("data"):
                break
                
            # Yield single rows (inefficient)
            for item in data["data"]:
                yield item
            
            page += 1
            if page > data.get("total_pages", 1):
                break

    @dlt.resource(name="products", write_disposition="replace")
    def get_products():
        client = RESTClient(base_url="https://jaffle-shop.scalevector.ai/api/v1")
        
        page = 1
        while True:
            response = client.get("/products", params={"page": page, "limit": 100})
            data = response.json()
            
            if not data.get("data"):
                break
                
            # Yield single rows (inefficient) 
            for item in data["data"]:
                yield item
            
            page += 1
            if page > data.get("total_pages", 1):
                break

    return [get_customers, get_orders, get_products]

# OPTIMIZED VERSION
def jaffle_shop_source_optimized():
    """Optimized implementation with chunking, parallelism, and performance tuning"""
    
    @dlt.resource(
        name="customers", 
        write_disposition="replace",
        parallelized=True
    )
    def get_customers() -> Iterator[Dict[str, Any]]:
        client = RESTClient(
            base_url="https://jaffle-shop.scalevector.ai/api/v1",
            paginator=JSONResponsePaginator(
                next_url_path="next_page_url"
            )
        )
        
        page = 1
        while True:
            response = client.get("/customers", params={"page": page, "limit": 1000})  # Larger chunks
            data = response.json()
            
            if not data.get("data"):
                break
                
            # Yield entire pages/chunks (efficient)
            yield data["data"]
            
            page += 1
            if page > data.get("total_pages", 1):
                break
    
    @dlt.resource(
        name="orders", 
        write_disposition="replace",
        parallelized=True
    )
    def get_orders() -> Iterator[Dict[str, Any]]:
        client = RESTClient(
            base_url="https://jaffle-shop.scalevector.ai/api/v1",
            paginator=JSONResponsePaginator(
                next_url_path="next_page_url"
            )
        )
        
        page = 1
        while True:
            response = client.get("/orders", params={"page": page, "limit": 1000})  # Larger chunks
            data = response.json()
            
            if not data.get("data"):
                break
                
            # Yield entire pages/chunks (efficient)
            yield data["data"]
            
            page += 1
            if page > data.get("total_pages", 1):
                break

    @dlt.resource(
        name="products", 
        write_disposition="replace",
        parallelized=True
    )
    def get_products() -> Iterator[Dict[str, Any]]:
        client = RESTClient(
            base_url="https://jaffle-shop.scalevector.ai/api/v1",
            paginator=JSONResponsePaginator(
                next_url_path="next_page_url"
            )
        )
        
        page = 1
        while True:
            response = client.get("/products", params={"page": page, "limit": 1000})  # Larger chunks
            data = response.json()
            
            if not data.get("data"):
                break
                
            # Yield entire pages/chunks (efficient)
            yield data["data"]
            
            page += 1
            if page > data.get("total_pages", 1):
                break

    return [get_customers, get_orders, get_products]

def run_performance_comparison():
    """Run both versions and compare performance"""
    
    print("=== PERFORMANCE COMPARISON ===\n")
    
    # Test naive version
    print("1. Running NAIVE version...")
    pipeline_naive = dlt.pipeline(
        pipeline_name="jaffle_shop_naive",
        destination="duckdb",
        dataset_name="jaffle_shop_naive"
    )
    
    start_time = time.time()
    source_naive = jaffle_shop_source_naive()
    info_naive = pipeline_naive.run(source_naive)
    naive_time = time.time() - start_time
    
    print(f"Naive version completed in: {naive_time:.2f} seconds")
    print(f"Naive trace: {pipeline_naive.last_trace}")
    print()
    
    # Test optimized version  
    print("2. Running OPTIMIZED version...")
    pipeline_optimized = dlt.pipeline(
        pipeline_name="jaffle_shop_optimized", 
        destination="duckdb",
        dataset_name="jaffle_shop_optimized"
    )
    
    start_time = time.time()
    source_optimized = jaffle_shop_source_optimized()
    info_optimized = pipeline_optimized.run(source_optimized)
    optimized_time = time.time() - start_time
    
    print(f"Optimized version completed in: {optimized_time:.2f} seconds")
    print(f"Optimized trace: {pipeline_optimized.last_trace}")
    print()
    
    # Performance analysis
    improvement = ((naive_time - optimized_time) / naive_time) * 100
    print("=== RESULTS ===")
    print(f"Naive time: {naive_time:.2f}s")
    print(f"Optimized time: {optimized_time:.2f}s")
    print(f"Performance improvement: {improvement:.1f}%")
    print()
    
    # Show load statistics
    print("=== LOAD STATISTICS ===")
    print("Naive version:")
    if info_naive.loads_ids:
        print(f"- Loads: {len(info_naive.loads_ids)}")
        print(f"- Load info: {info_naive}")
    
    print("\nOptimized version:")
    if info_optimized.loads_ids:
        print(f"- Loads: {len(info_optimized.loads_ids)}")
        print(f"- Load info: {info_optimized}")
    
    # Verify data was loaded correctly
    print("\n=== DATA VERIFICATION ===")
    try:
        import duckdb
        
        # Check naive version
        conn_naive = duckdb.connect("jaffle_shop_naive.duckdb")
        customers_count = conn_naive.execute("SELECT COUNT(*) FROM jaffle_shop_naive.customers").fetchone()[0]
        orders_count = conn_naive.execute("SELECT COUNT(*) FROM jaffle_shop_naive.orders").fetchone()[0]  
        products_count = conn_naive.execute("SELECT COUNT(*) FROM jaffle_shop_naive.products").fetchone()[0]
        print(f"Naive - Customers: {customers_count}, Orders: {orders_count}, Products: {products_count}")
        
        # Check optimized version
        conn_opt = duckdb.connect("jaffle_shop_optimized.duckdb")
        customers_count_opt = conn_opt.execute("SELECT COUNT(*) FROM jaffle_shop_optimized.customers").fetchone()[0]
        orders_count_opt = conn_opt.execute("SELECT COUNT(*) FROM jaffle_shop_optimized.orders").fetchone()[0]
        products_count_opt = conn_opt.execute("SELECT COUNT(*) FROM jaffle_shop_optimized.products").fetchone()[0]
        print(f"Optimized - Customers: {customers_count_opt}, Orders: {orders_count_opt}, Products: {products_count_opt}")
        
        # Verify counts match
        if (customers_count == customers_count_opt and 
            orders_count == orders_count_opt and 
            products_count == products_count_opt):
            print("Data consistency verified, both versions loaded same amount of data")
        else:
            print("Data mismatch detected!")
            
    except Exception as e:
        print(f"Could not verify data: {e}")

def main():
    """Main execution function"""
    print("Jaffle Shop dlt Pipeline, Performance Optimization")
    print("=" * 60)
    
    # Run performance comparison
    run_performance_comparison()
    
    print("\n=== KEY OPTIMIZATIONS APPLIED ===")
    print("1. Chunking: Yielding entire pages (1000 items) instead of single rows")
    print("2. Parallelism: Using parallelized=True on all resources") 
    print("3. Worker tuning: 4 workers each for extract, normalize, load")
    print("4. Buffer control: Increased buffer sizes and file rotation limits")
    print("5. RESTClient: Using proper REST client with pagination support")
    print("6. Efficient pagination: Larger page sizes (1000 vs 100)")

if __name__ == "__main__":
    main()
