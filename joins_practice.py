"""
PySpark fizikai join stratégiák gyakorlása

Spark 4 fő stratégiát választhat (vagy kényszeríthetsz hint-tel):

  1. BroadcastHashJoin    — kis tábla szétküldve minden executorra, hash lookup
  2. ShuffledHashJoin     — mindkét tábla shuffle-öl join key szerint, hash tábla épül
  3. SortMergeJoin        — mindkét tábla shuffle + sort, majd merge (default nagy táblákhoz)
  4. BroadcastNestedLoopJoin — non-equi join esetén, minden sor × minden sor (lassú!)

Hint szintaxis: df.hint("BROADCAST") / df.hint("SHUFFLE_HASH") / stb.
Explain:       df.explain("formatted")  — megmutatja Spark melyiket választotta
"""

import os
from pyspark.sql.functions import broadcast, col
from etl.helpers import get_spark_session, read_csv

BASE_DIR = os.path.dirname(os.path.abspath(__file__))

spark = get_spark_session("joins-physical-strategies")

products = read_csv(spark, os.path.join(BASE_DIR, "products.csv"))
orders = read_csv(spark, os.path.join(BASE_DIR, "orders.csv"))
customers = read_csv(spark, os.path.join(BASE_DIR, "customers.csv"))

# ---------------------------------------------------------------------------
# 1. BROADCAST HASH JOIN
#
#    Mikor: egyik tábla kicsi (default küszöb: 10 MB, spark.sql.autoBroadcastJoinThreshold)
#    Hogyan működik:
#      - kis tábla teljes egészében elküldi (broadcast) MINDEN executornak
#      - nagy tábla sorait hash lookup-pal illeszti — nincs shuffle!
#    Előny: shuffle nélkül → gyors, network-hatékony
#    Hátrány: ha a "kis" tábla mégsem fér memóriába → OOM
#
#    Kényszerítés: broadcast(df) függvénnyel VAGY hint-tel
# ---------------------------------------------------------------------------
print("=== 1. BROADCAST HASH JOIN ===")
print("customers tábla broadcast-olva minden executorra. Nincs shuffle.")

result = orders.join(broadcast(customers), on="customer_id", how="inner")
result.explain("formatted")
result.select("order_id", "customer_id", "name", "city", "quantity").show()

# Alternatív hint szintaxis (ugyanaz):
# orders.join(customers.hint("BROADCAST"), on="customer_id", how="inner")

# ---------------------------------------------------------------------------
# 2. SHUFFLE HASH JOIN
#
#    Mikor: mindkét tábla közepes méretű; egyik partition szintjén befér memóriába
#    Hogyan működik:
#      - mindkét tábla shuffle-öl join key szerint (azonos key → azonos partíció)
#      - kisebb oldalból hash tábla épül partition-ként
#      - nagy oldal sorait hash lookup-pal illeszti
#    Előny: sort nem kell (gyorsabb mint SortMerge ha adat fér memóriába)
#    Hátrány: hash tábla memóriában kell legyen; ha spill → lassú
#
#    Kényszerítés: SHUFFLE_HASH hint
# ---------------------------------------------------------------------------
print("=== 2. SHUFFLE HASH JOIN ===")
print("Shuffle join key szerint, hash tábla partition-ként. Sort nélkül.")

result = orders.join(products.hint("SHUFFLE_HASH"), on="product_id", how="inner")
result.explain("formatted")
result.select("order_id", "product_id", "name", "quantity").show()

# ---------------------------------------------------------------------------
# 3. SORT MERGE JOIN
#
#    Mikor: ALAPÉRTELMEZETT nagy tábláknál (egyik sem broadcast-olható)
#    Hogyan működik:
#      - mindkét tábla shuffle-öl join key szerint
#      - mindkét oldal SORTOL join key szerint
#      - két rendezett lista merge-elése (mint merge sort utolsó lépése)
#    Előny: nagy táblákra is stabil; nem kell az egész hash tábla memóriában
#    Hátrány: sort lépés plusz overhead; shuffle mindenképp van
#
#    Kényszerítés: SHUFFLE_MERGE hint
#    Letiltás:     spark.conf.set("spark.sql.join.preferSortMergeJoin", "false")
# ---------------------------------------------------------------------------
print("=== 3. SORT MERGE JOIN ===")
print("Default nagy táblákhoz. Shuffle + sort + merge lépések.")

result = orders.hint("SHUFFLE_MERGE").join(products, on="product_id", how="inner")
result.explain("formatted")
result.select("order_id", "product_id", "name", "quantity").show()

# ---------------------------------------------------------------------------
# 4. BROADCAST NESTED LOOP JOIN
#
#    Mikor: nincs equi-join feltétel (pl. range join, CROSS JOIN, >, <, !=)
#    Hogyan működik:
#      - kis tábla broadcast-olva
#      - minden sor a nagy táblából összehasonlítva minden broadcast sorral
#      - O(n × m) komplexitás → LASSÚ nagy adatokon!
#    Kényszerítés: SHUFFLE_REPLICATE_NL hint
# ---------------------------------------------------------------------------
print("=== 4. BROADCAST NESTED LOOP JOIN (non-equi) ===")
print("Nincs egyenlőség-feltétel. Minden sor × minden sor összehasonlítás.")

# Példa: melyik rendelés drágább termékre szól, mint 100 HUF?
result = orders.join(
    products.hint("SHUFFLE_REPLICATE_NL"),
    on=col("orders.product_id") == col("products.product_id"),
    how="inner"
).filter(col("price") > 100)
result.explain("formatted")
result.select("order_id", "name", "price", "quantity").orderBy("price").show()

# ---------------------------------------------------------------------------
# AUTO BROADCAST KÜSZÖB — konfiguráció
#
#    Spark automatikusan broadcast-ol ha tábla < küszöb (default 10MB = 10485760 byte)
#    Kikapcsolás: -1
#    Növelés pl. 50MB-ra: 52428800
# ---------------------------------------------------------------------------
print("=== AUTO BROADCAST KÜSZÖB ===")
threshold = spark.conf.get("spark.sql.autoBroadcastJoinThreshold")
print(f"Jelenlegi küszöb: {threshold} byte ({int(threshold) // 1024 // 1024} MB)")
print("Kikapcsoláshoz: spark.conf.set('spark.sql.autoBroadcastJoinThreshold', '-1')")
print("Növeléshez:     spark.conf.set('spark.sql.autoBroadcastJoinThreshold', '52428800')")

# ---------------------------------------------------------------------------
# ÖSSZEFOGLALÓ TÁBLÁZAT
# ---------------------------------------------------------------------------
print("""
+------------------------+------------------+----------------------------------+------------------+
| Stratégia              | Shuffle kell?    | Mikor Spark ezt választja        | Hint             |
+------------------------+------------------+----------------------------------+------------------+
| BroadcastHashJoin      | NEM              | kis tábla < autoBroadcast küszöb | BROADCAST        |
| ShuffledHashJoin       | IGEN             | közepes tábla, fér hash memóriába| SHUFFLE_HASH     |
| SortMergeJoin          | IGEN             | nagy tábla (default)             | SHUFFLE_MERGE    |
| BroadcastNestedLoopJoin| részben          | non-equi join / cross join       | SHUFFLE_REPLICATE_NL |
+------------------------+------------------+----------------------------------+------------------+
""")

spark.stop()
