# ENRON-EMAILS-AND-EMPLOYEE




## THE ENRON DATA SET

The size of raw data is too large to be allowed uploading and it can be found on the website https://www.cs.cmu.edu/~./enron/.

Here we thank Brian Ray for sharing the preprocessed code on the websit:https://github.com/brianray/data.world-scripts/blob/master/enron_email_script.ipynb. This code can change the raw data into a csv file. If you skip this step, you could download the data enron_05_17_2015_with_labels_v2.csv-enron_05_17_2015_with_labels_v2 on the website https://data.world/brianray/enron-email-dataset 

## THE NAMELIST OF EMPLOYEE

Position : *input/enron_emp.csv*

## THE NAMELIST OF CRIMINALS

Position : *input/criminals.csv*

We sum up the name list of criminals from the website *http://usatoday30.usatoday.com/money/industries/energy/2005-12-28-enron-participants_x.htm*. 

## PRE-PROCESSING THE DATA SET

Position: *enron_emails.py* 

This code is used to filter emails according to the email addresses of the employees.
The raw number of emails is **517407** and the number of emails after pre-processing is **47468**.
