from pyspark import SparkContext, SparkConf

conf = SparkConf().setAppName('dailyRevenuePerProduct').setMaster('yarn-client')
sc = SparkContext(conf=conf)

orders = sc.textFile("/public/retail_db/orders",10)
orderItems = sc.textFile("/public/retail_db/order_items",10)
products = sc.textFile("/public/retail_db/products",10)

ordersMap = orders.filter(lambda x : x.split(",")[3] in ['COMPLETE','CLOSED']) \
                  .map(lambda x : (int(x.split(',')[0]), x.split(',')[1]))
# ordersMap : (orderId, orderDate)
orderItemsMap = orderItems.map(lambda x : (int(x.split(',')[1]), \
                                        (int(x.split(",")[2]), float(x.split(',')[4]))))
# OrderItemsMap : (orderId, (productId, subtotal))
productsMap = products.map(lambda x : (int(x.split(',')[0]), x.split(',')[2]))
# productsMap : (productId, ProductName)


ordersJoin = ordersMap.join(orderItemsMap)
# ordersJoin : (orderId, (orderDate, (productId,subtotal)))

ordersJoinMap = ordersJoin.map(lambda x : ((x[1][0], x[1][1][0]), x[1][1][1]))
# ordersJoinMap : ((orderDate, productId), subtotal)

dailyRevenuePerProductId = ordersJoinMap.reduceByKey(lambda x,y : x + y)
# dailyRevenuePerProductId : ((orderDate, ProductId), sum of subtotal per productId & Date)

dailyRevenuePerProductIdMap = dailyRevenuePerProductId.map(lambda x : (x[0][1],(x[0][0], x[1])))
# dailyRevenuePerProductIdMap : (productId, (orderDate, sum of subtotal))
# productsMap : (productId, productName)

dailyRevenuePerProductIdMapJoin = dailyRevenuePerProductIdMap.join(productsMap)
# dailyRevenuePerProductIdMapJoin : (productId, ((orderDate, sum of subtotal), productName))

dailyRevenuePerProductName = dailyRevenuePerProductIdMapJoin \
    .map(lambda x: (x[1][1][0], -x[1][1][1]), x[1][1][0]+','+str(x[1][1][1])+','+x[1][1]) \
    .sortByKey()
#dailyRevenuePerProductName : ((orderDate, -sum of subtotal), orderDate,sum of subtotal,productName)
# Here it is sored by orderDate(asc), sum of subtotal (desc) as per the requirement.

dailyRevenuePerProductNameFinal = dailyRevenuePerProductName.map(lambda x : x[1])
# dailyReveuePerProductNameFinal : orderDate, sum of subtotal, product Name

dailyRevenuePerProductNameFinal.coalesec(2).saveAsTextFile('/user/karthik2403/dailyRevenue_txt')


