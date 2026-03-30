spark.conf.set(
  "fs.azure.sas.files.samplestorageaccount551.blob.core.windows.net",
  "sp=rw&st=2026-03-30T10:58:35Z&se=2026-03-30T19:13:35Z&spr=https&sv=2024-11-04&sr=b&sig=EnbtVpNhC13vMjeIWKPPJzDT1iPQEk7NmgLy5gPUqh8%3D"
)

df = spark.read.option("header", "true").csv(
  "wasbs://files@samplestorageaccount551.blob.core.windows.net/EmploeeData.csv"
)

display(df)