from setuptools import setup, find_packages

with open(file="README.md", mode="r") as readme_handle:
    long_description = readme_handle.read()

setup(
    name='job_offers_data_load',
    version='0.0.1',
    description='Part that inserts data to MongoDB and PostgreSQL.',
    long_description=long_description,
    long_description_content_type="text/markdown",
    packages=find_packages(),
    python_requires=">=3.6",
    install_requires=[
        'pandas==1.3.2',
        'SQLAlchemy==1.4.22',
        'shortuuid==1.0.1',
        'pymongo==3.12.0',
        'python-dotenv~=0.19.0',
        'psycopg2==2.9.1',
    ],
)
