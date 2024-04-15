from pystitchr.registry.service import *
from pystitchr.util.database.column import *

r = RegistrySchema()
r.initialize_datacatalog_views()
df = r.get_schema_df(1)

df.show()
# df.select("column_type").show()

sc = r.get_schema(r.get_schema_df(1))
print(sc)

