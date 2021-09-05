import uuid
import os
from cassandra.auth import PlainTextAuthProvider
from cassandra.cluster import Cluster
import csv
import scipy.stats

# setup an Astra Client and create a shortcut to our test colllection
cloud_config= {
        'secure_connect_bundle': 'secure-connect-owid-data.zip'
}
auth_provider = PlainTextAuthProvider('AKujsCvecqQcxukwiOhmsmRa', 'IYh6xUy+D01YB8Mc2KdZvppExBAgkpLzbgvzP0-DnrI3,C0+A-3Uc0jRAoBjmErRSY1Q5Pz3gO5gco_cKiudRR+DBfSlYgjEBvObnJBu4vQshjbC,7x+Or,iFdC+sSHO')
cluster = Cluster(cloud=cloud_config, auth_provider=auth_provider)
session = cluster.connect('OWID')

session.execute(
    """CREATE TABLE IF NOT EXISTS pc_data_global3 (
        location text PRIMARY KEY,
        pc_by_location float
    );
    """
)

data = {'positive_rate':[], 'total_vaccinations_per_hundred':[], 'location':[], 'pc_by_location':[], 'prate_2':[], 'vperc_2':[]}
posx = -1
with open("owid-covid-data.csv", "r") as fp:
    reader = csv.DictReader(fp)
    for i in reader:
        if posx >= 0 and data["location"][posx] == str(i["location"]):
            pass
        else:
            if posx >= 0:
                
                if len(data["prate_2"])>=2 and len(data["vperc_2"])>=2:
                    pc_by_location = (scipy.stats.pearsonr(data["prate_2"], data["vperc_2"]))[0]
                    if pc_by_location is None:
                        data["pc_by_location"].append(0)
                    else:
                        data["pc_by_location"].append(pc_by_location)
                else:
                    pc_by_location = (scipy.stats.pearsonr([0,0] , [0,0]))[0]
                    if pc_by_location is None:  
                        data["pc_by_location"].append(0)
                    else:
                        data["pc_by_location"].append(pc_by_location)
                    
                     
            else:
                pass
                
            data["location"].append(i["location"])
            data["prate_2"].clear()
            data["vperc_2"].clear()
            posx = posx + 1
        if  i["positive_rate"] == '':
            data["positive_rate"].append(0)
            data["prate_2"].append(0)
        else:
            data["positive_rate"].append(float(i["positive_rate"]))
            data["prate_2"].append(float(i["positive_rate"]))
        if i["total_vaccinations_per_hundred"] == '':
            data["total_vaccinations_per_hundred"].append(0)
            data["vperc_2"].append(0)
        else:
            data["total_vaccinations_per_hundred"].append(float(i["total_vaccinations_per_hundred"]))
            data["vperc_2"].append(float(i["total_vaccinations_per_hundred"]))
      

"""
Now we calculate the  correlation using Pearson`s correlation formula. \
We calculate the correlation by taking X = positivity rate and Y = vaccination percentage
"""

#Pearson`s Correlation

pc_global = scipy.stats.pearsonr(data["positive_rate"], data["total_vaccinations_per_hundred"])[0]

print(f"The pearson correlation gives a factor of {pc_global}. This shows a negative correlation between the Positivity rate and total percentage of people who have recieved the vaccine. We go further and give a list of Pearson`s Correlation of different countries. This shows the net efffect on vaccination on the positivity rate.")
      
session.execute(f"""
      INSERT INTO pc_data_global3 ("location", "pc_by_location")
      VALUES ('world', {pc_global});
"""
)
    

for i in range(len(data["location"])):
    if data["location"][i] != "Zambia":
        print(f"{data['location']} : {data['pc_by_location']}")
        session.execute(f"""
                INSERT INTO pc_data_global3 ("location", "pc_by_location")
                VALUES ('{data["location"][i]}', 
                {data["pc_by_location"][i]});
        """
        )   
    else:
        print(f"{data['location']} : {data['pc_by_location']}")
        session.execute(f"""
                INSERT INTO pc_data_global3 ("location", "pc_by_location")
                VALUES ('{data["location"][i]}%', 
                {data["pc_by_location"][i]});
        """
        )
        break;
print("\n This shows with a constant effort we can get the positivity rate of covid down by increasing vaccination drives throughout the globe")            
print("The Rows are added successfully. You can view the data on AstraDB")
            
        
            
