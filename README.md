# Pearson-r-covid-vaccination
The pearson correlation gives a correlation factor. This shows a negative correlation between the Positivity rate and total percentage of people who have recieved the vaccine. We go further and give a list of Pearson`s Correlation of different countries. This shows the net efffect on vaccination on the positivity rate.

##Building the program
First the repository needs to be cloned. Assuming Version of python is higher than 3.6. You would need cassandra and scipy libraries. It can  be installed via pip
e.g pip install scipy
Then to run the python script you need to connect to AstaDB. Registering in AstraDB is simple. You can register at AstraDb. Create your first database and create client Id and Client secret and app token at the token section.

Then after initializing the database replace my credentials with your own and run it using 'python correlation.py'. It would take some time. (Note: OWID csv data needs to be in  the same directory)

The script will run and print the values and will also store it in AstraDB.

## Contact

For any queries. Contact me at pranavupadhyaya51@gmail.com


