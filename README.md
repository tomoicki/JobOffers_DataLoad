## JobOffers_DataLoad
Data load part that inserts data to MongoDB and PostgreSQL
- [Data Extraction](https://github.com/tomoicki/JobOffers_DataExtraction)
- [Data Transformation](https://github.com/tomoicki/JobOffers_DataTransformation)
- Data Load (you are here)
- [REST API](https://github.com/tomoicki/JobOffers_API)

To use it u should already have MongoDB and PostgresSQL databases created either locally or on remote host.

Databases credentials are required to make connection.
For this purpose we created .env file that contains environment variables holding databases credentials.

To use this approach you also have to install python-dotenv library which we omitted in requirements.txt

>pip install python-dotenv

Contact me directly for .env file.

See example_use.py for details.