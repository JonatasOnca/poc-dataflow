import setuptools

setuptools.setup(
    name="mysql-to-bq-dataflow-multiple-tables",
    version="1.0.0",
    install_requires=[
        "apache-beam[gcp]==2.68.0",
        "pyarrow==18.1.0",
        "google-cloud-bigquery==3.38.0",
        "google-cloud-secret-manager==2.24.0",
        "PyYAML==6.0.3",
        "Jinja2==3.1.6",
        "mysql-connector-python==9.4.0",
    ],
    packages=setuptools.find_packages(),
    description="Pipeline Dataflow para ingestão incremental do MySQL para o GCS.",
)
