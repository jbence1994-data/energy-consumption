"""
PySpark Join típusok gyakorlása

Adatok:
  products.csv  — 14 termék (product_id 1-14)
  orders.csv    — 10 rendelés
                   - order 8,9: product_id 99/100 NEM létezik → outer join különbség látszik
                   - product 2,4,6,8,10,14: nincs hozzájuk rendelés
  customers.csv — 6 vevő
                   - customer 5,6: nincs rendelésük → anti/semi join látszik
                   - order 9: customer_id 7 NEM létezik

Szintaxis: df1.join(df2, on=<feltétel>, how=<típus>)
"""

import os
from etl.helpers import get_spark_session, read_csv

BASE_DIR = os.path.dirname(os.path.abspath(__file__))

spark = get_spark_session("joins-practice")

products  = read_csv(spark, os.path.join(BASE_DIR, "products.csv"))
orders    = read_csv(spark, os.path.join(BASE_DIR, "orders.csv"))
customers = read_csv(spark, os.path.join(BASE_DIR, "customers.csv"))

print("=== ALAPTÁBLÁK ===")
print("-- products (14 sor) --")
products.select("product_id", "name", "category", "price").show()
print("-- orders (10 sor) --")
orders.show()
print("-- customers (6 sor) --")
customers.show()

# ---------------------------------------------------------------------------
# 1. INNER JOIN
#    Csak azok a rendelések, amelyekhez létező termék tartozik.
#    SQL: SELECT * FROM orders JOIN products ON orders.product_id = products.product_id
#    Eredmény: 8 sor (order 8 és 9 kiesik, mert product_id 99/100 nem létezik)
# ---------------------------------------------------------------------------
print("=== 1. INNER JOIN (orders + products) ===")
print("Csak egyező sorok mindkét oldalon. Order 8/9 kiesik (product 99/100 hiányzik).")
(orders
    .join(products, on="product_id", how="inner")
    .select("order_id", "product_id", "name", "customer_id", "quantity")
    .orderBy("order_id")
    .show())

# ---------------------------------------------------------------------------
# 2. LEFT OUTER JOIN  (alias: "left")
#    Bal tábla (orders) ÖSSZES sora megmarad.
#    Ha a jobb oldalon (products) nincs egyezés → null.
#    SQL: SELECT * FROM orders LEFT JOIN products ON ...
#    Eredmény: 10 sor — order 8/9-nél name=null, price=null
# ---------------------------------------------------------------------------
print("=== 2. LEFT OUTER JOIN (orders LEFT, products RIGHT) ===")
print("Összes rendelés megmarad. Hiányzó terméknél null jelenik meg.")
(orders
    .join(products, on="product_id", how="left")
    .select("order_id", "product_id", "name", "customer_id", "quantity")
    .orderBy("order_id")
    .show())

# ---------------------------------------------------------------------------
# 3. RIGHT OUTER JOIN  (alias: "right")
#    Jobb tábla (products) ÖSSZES sora megmarad.
#    Ha a bal oldalon (orders) nincs egyezés → null.
#    SQL: SELECT * FROM orders RIGHT JOIN products ON ...
#    Eredmény: 14+ sor — product 2,4,6,8,10,14-nél order_id=null
# ---------------------------------------------------------------------------
print("=== 3. RIGHT OUTER JOIN (orders LEFT, products RIGHT) ===")
print("Összes termék megmarad. Rendelt termékek mellé null kerül az order mezőkbe.")
(orders
    .join(products, on="product_id", how="right")
    .select("order_id", "product_id", "name", "customer_id", "quantity")
    .orderBy("product_id")
    .show())

# ---------------------------------------------------------------------------
# 4. FULL OUTER JOIN  (alias: "outer", "full", "fullouter")
#    MINDKÉT tábla összes sora megjelenik.
#    Ahol nincs egyezés (akár bal, akár jobb oldalon) → null.
#    SQL: SELECT * FROM orders FULL OUTER JOIN products ON ...
#    Eredmény: order 8/9 (nincs product) + product 2/4/6/8/10/14 (nincs order) is látszik
# ---------------------------------------------------------------------------
print("=== 4. FULL OUTER JOIN (orders + products) ===")
print("Minden sor megjelenik mindkét táblából. Hiányzó oldal = null.")
(orders
    .join(products, on="product_id", how="outer")
    .select("order_id", "product_id", "name", "customer_id", "quantity")
    .orderBy("product_id")
    .show(20))

# ---------------------------------------------------------------------------
# 5. LEFT SEMI JOIN  (alias: "leftsemi", "semi")
#    Bal tábla azon sorai, amelyeknek VAN egyező jobb oldali soruk.
#    Jobb tábla oszlopai NEM kerülnek be az eredménybe.
#    SQL ekvivalens: SELECT * FROM products WHERE product_id IN (SELECT product_id FROM orders)
#    Eredmény: 8 termék — csak azok, amelyekre érkezett rendelés
# ---------------------------------------------------------------------------
print("=== 5. LEFT SEMI JOIN (products LEFT, orders RIGHT) ===")
print("Termékek, amelyekre VAN rendelés. Jobb tábla (orders) oszlopai nem jelennek meg.")
(products
    .join(orders, on="product_id", how="leftsemi")
    .select("product_id", "name", "category")
    .orderBy("product_id")
    .show())

# ---------------------------------------------------------------------------
# 6. LEFT ANTI JOIN  (alias: "leftanti", "anti")
#    Bal tábla azon sorai, amelyeknek NINCS egyező jobb oldali soruk.
#    A SEMI join ellentéte.
#    SQL ekvivalens: SELECT * FROM products WHERE product_id NOT IN (SELECT product_id FROM orders)
#    Eredmény: 6 termék — amelyekre soha nem érkezett rendelés
# ---------------------------------------------------------------------------
print("=== 6. LEFT ANTI JOIN (products LEFT, orders RIGHT) ===")
print("Termékek, amelyekre NINCS rendelés. SEMI join ellentéte.")
(products
    .join(orders, on="product_id", how="leftanti")
    .select("product_id", "name", "category")
    .orderBy("product_id")
    .show())

# ---------------------------------------------------------------------------
# 7. CROSS JOIN  (Descartes-szorzat)
#    Bal tábla MINDEN sora kombinálva jobb tábla MINDEN sorával.
#    Feltétel NINCS — n × m sor lesz az eredmény.
#    Hasznos: kombinációk generálásához, de nagy táblákon veszélyes!
#    customers (6) × products (14) = 84 sor
# ---------------------------------------------------------------------------
print("=== 7. CROSS JOIN (customers × products) ===")
print("Minden vevő × minden termék kombináció. 6 × 14 = 84 sor.")
(customers
    .join(products, how="cross")
    .select("customer_id", customers["name"].alias("customer"), "product_id", products["name"].alias("product"), "price")
    .orderBy("customer_id", "product_id")
    .show(10))
print("(csak első 10 sor látszik a 84-ből)")

# ---------------------------------------------------------------------------
# BÓNUSZ: Összetett join feltétel + több táblás join
#    orders + products + customers egyszerre
# ---------------------------------------------------------------------------
print("=== BÓNUSZ: Háromtáblás join (orders + products + customers) ===")
print("Teljes rendelési lista: rendelő neve, termék neve, ár, darab.")
(orders
    .join(products, on="product_id", how="inner")
    .join(customers, on="customer_id", how="left")
    .select(
        "order_id",
        customers["name"].alias("customer_name"),
        "city",
        products["name"].alias("product_name"),
        "price",
        "quantity"
    )
    .orderBy("order_id")
    .show())

spark.stop()
