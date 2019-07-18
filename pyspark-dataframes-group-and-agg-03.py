# Get daily product revenue 
# filter for complete and closed orders
# groupBy order_date and order_item_product_id
# Use agg and sum on order_item_subtotal to get revenue

spark.conf.set('spark.sql.shuffle.partitions', '2')

from pyspark.sql.functions import sum, round
orders.where('order_status in ("COMPLETE", "CLOSED")'). \
  join(orderItems, orders.order_id == orderItems.order_item_order_id). \
  groupBy('order_date', 'order_item_product_id'). \
  agg(round(sum('order_item_subtotal'), 2).alias('revenue')). \
  show()

orders.where('order_status in ("COMPLETE", "CLOSED")'). \
  join(orderItems, orders.order_id == orderItems.order_item_order_id). \
  groupBy(orders.order_date, orderItems.order_item_product_id). \
  agg(round(sum(orderItems.order_item_subtotal), 2).alias('revenue')). \
  show()
