# pipeline.py

import dlt
from dlt.sources.rest_api import rest_api_source

# Define source
def jaffle_shop_source():
    return rest_api_source({
        "client": {
            "base_url": "https://jaffle-shop.scalevector.ai/api/v1"
        },
        "resources": [
            {
                "name": "orders",
                "endpoint": {
                    "path": "/orders",
                    "params": {
                        "limit": 100
                    },
                    "data_selector": "data",
                    "incremental": {
                        "cursor_path": "ordered_at",
                        "initial_value": "2017-08-01T00:00:00Z"
                    }
                },
                "primary_key": "id",
                "write_disposition": "merge"
            }
        ]
    })

# Add filtering 
def filter_orders(item):
    """Filter orders with order_total <= 500"""
    return item.get('order_total', 0) <= 500

# Run pipeline
pipeline = dlt.pipeline(
    pipeline_name="jaffle_shop_incremental",
    destination="duckdb",
    dataset_name="jaffle_shop"
)

# Extract, filter, normalize, then load
source = jaffle_shop_source()
source.orders.add_filter(filter_orders)

info = pipeline.run(source)

# Print results
print(info)

# Optional: Show some data
if info.loads_ids:
    print("First few orders loaded (with order_total <= 500):")
    import duckdb
    conn = duckdb.connect("jaffle_shop_incremental.duckdb")
    result = conn.execute("SELECT ordered_at, order_total FROM orders ORDER BY ordered_at LIMIT 5").fetchall()
    for row in result:
        print(row)
