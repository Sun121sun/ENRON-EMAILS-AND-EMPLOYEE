
# coding: utf-8

#!/usr/bin/python3
import os, sys, email, string
import types
import numpy as np 
import pandas as pd
from boto.s3.key import Key
import boto
import zipfile
import re

from subprocess import check_output

#all 50W emails
emails=pd.read_csv('input/enron_05_17_2015_with_labels_v2.csv-enron_05_17_2015_with_labels_v2.csv',usecols=[0,1,2,3,4,5,6,7,13,14,51])

anrn_emp=pd.read_csv('input/enron_emp.csv')

anrn_emp=anrn_emp.drop_duplicates()
def to_upper(s):
    s=s.upper()
    s=s.strip()
    s=' '.join(s.split())
    return s
def del_a(s):
    if s != '':
        s =str(s)
        s = re.sub('frozenset\(\{','',s)
        s = re.sub('\}\)','',s)
        s = re.sub('\'','',s)
        s = re.sub('\"','',s)
    return s

emails['From']=emails['From'].apply(del_a)
emails['To']=emails['To'].apply(del_a)


print(anrn_emp['email1'].head())
#name together
def last_to(s):
    s=str(s)

    if s.find('k..allen@enron.com')!=-1:
        s=re.sub('k..allen@enron.com','phillip.allen@enron.com',s)
        #print ('1')
    if s.find('f..brawner@enron.com')!=-1:
        s=re.sub('f..brawner@enron.com','sandra.brawner@enron.com',s)
        #print ('2')
    if s.find('f..campbell@enron.com')!=-1:
        s=re.sub('f..campbell@enron.com','larry.campbell@enron.com',s)
        #print ('3')
    if s.find('e..haedicke@enron.com')!=-1:
        s=re.sub('e..haedicke@enron.com','mark.haedicke@enron.com',s)
        #print ('4')
    if s.find('mark.e.haedicke@enron.com')!=-1:
        s=re.sub('mark.e.haedicke@enron.com','mark.haedicke@enron.com',s)
        #print ('41')
    if s.find('w..delainey@enron.com')!=-1:
        s=re.sub('w..delainey@enron.com','david.delainey@enron.com',s)
        #print ('5')
    if s.find('j..farmer@enron.com')!=-1:
        s=re.sub('j..farmer@enron.com','daren.farmer@enron.com',s)
        #print ('6')
    if s.find('j..kaminski@enron.com')!=-1:
        s=re.sub('j..kaminski@enron.com','vince.kaminski@enron.com',s)
        #print ('7')
    if s.find('j.kaminski@enron.com')!=-1:
        s=re.sub('j.kaminski@enron.com','vince.kaminski@enron.com',s)
        #print ('71')
    if s.find('kaminski@enron.com')!=-1:
        s=re.sub('kaminski@enron.com','vince.kaminski@enron.com',s)
        #print ('72')
    if s.find('m..forney@enron.com')!=-1:
        s=re.sub('m..forney@enron.com','john.forney@enron.com',s)
        #print ('8')
    if s.find('l..gay@enron.com')!=-1:
        s=re.sub('l..gay@enron.com','randall.gay@enron.com',s)
        #print ('9')
    if s.find('t..hodge@enron.com')!=-1:
        s=re.sub('t..hodge@enron.com','jeffrey.hodge@enron.com',s)
        #print ('10')
    if s.find('c..giron@enron.com')!=-1:
        s=re.sub('c..giron@enron.com','darron.giron@enron.com',s)
        #print ('11')
    if s.find('horton@enron.com')!=-1:
        s=re.sub('horton@enron.com','stanley.horton@enron.com',s)
        #print ('12')
    if s.find('j..kean@enron.com')!=-1:
        s=re.sub('j..kean@enron.com','steven.kean@enron.com',s)
        #print ('13')

    if s.find('f..keavey@enron.com')!=-1:
        s=re.sub('f..keavey@enron.com','peter.keavey@enron.com',s)
        #print ('15')
    if s.find('lavorato@enron.com')!=-1:
        s=re.sub('lavorato@enron.com','john.lavorato@enron.com',s)
        #print ('16')
    if s.find('h..lewis@enron.com')!=-1:
        s=re.sub('h..lewis@enron.com','andrew.lewis@enron.com',s)
        #print ('17')
    if s.find('michele.lokay@enron.com')!=-1:
        s=re.sub('michele.lokay@enron.com','michelle.lokay@enron.com',s)
        #print ('18')
    if s.find( 'm..love@enron.com')!=-1:
        s=re.sub('m..love@enron.com','phillip.love@enron.com',s)
        #print ('19')
    if s.find( 'a..martin@enron.com')!=-1:
        s=re.sub('a..martin@enron.com','thomas.martin@enron.com',s)
        #print ('20')
    if s.find( 'l..mims@enron.com')!=-1:
        s=re.sub('l..mims@enron.com','patrice.mims@enron.com',s)
        #print ('21')
    if s.find( 'w..pereira@enron.com')!=-1:
        s=re.sub('w..pereira@enron.com','susan.pereira@enron.com',s)
        #print ('22')
    if s.find( 'm..presto@enron.com')!=-1:
        s=re.sub('m..presto@enron.com','kevin.presto@enron.com',s)
        #print ('23')
    if s.find( 'b..sanders@enron.com')!=-1:
        s=re.sub('b..sanders@enron.com','richard.sanders@enron.com',s)
        #print ('24')
    if s.find( 'm..scott@enron.com')!=-1:
        s=re.sub('m..scott@enron.com','susan.scott@enron.com',s)
        #print ('25')

    if s.find( 'a..shankman@enron.com')!=-1:
        s=re.sub('a..shankman@enron.com','jeffrey.shankman@enron.com',s)
        #print ('27')
    if s.find( 's..shively@enron.com')!=-1:
        s=re.sub('s..shively@enron.com','hunter.shively@enron.com',s)
        #print ('28')
    if s.find( 'd..steffes@enron.com')!=-1:
        s=re.sub('d..steffes@enron.com','james.steffes@enron.com',s)
        #print ('29')
    if s.find( 'j..sturm@enron.com')!=-1:
        s=re.sub('j..sturm@enron.com','fletcher.sturm@enron.com',s)
        #print ('30')
    if s.find( 'legal <.taylor@enron.com>')!=-1:
        s=re.sub('legal <.taylor@enron.com>','mark.taylor@enron.com',s)
        #print ('31')
    if s.find( 'e.taylor@enron.com')!=-1:
        s=re.sub('legal <.taylor@enron.com>','mark.taylor@enron.com',s)
        #print ('311')
    if s.find( 'm..tholt@enron.com')!=-1:
        s=re.sub( 'm..tholt@enron.com','jane.tholt@enron.com',s)
        #print ('32')
    if s.find( 'd..thomas@enron.com')!=-1:
        s=re.sub('d..thomas@enron.com','paul.thomas@enron.com',s)
        #print ('33')    
    if s.find( 's..ward@enron.com')!=-1:
        s=re.sub('s..ward@enron.com','kim.ward@enron.com',s)
        #print ('34')
    if s.find( 'v.weldon@enron.com')!=-1:
        s=re.sub('v.weldon@enron.com','charles.weldon@enron.com',s)
        #print ('35')
    if s.find( 'w..white@enron.com')!=-1:
        s=re.sub('w..white@enron.com','stacey.white@enron.com',s)
        #print ('36')
    if s.find( 'trading <.williams@enron.com>')!=-1:
        s=re.sub('trading <.williams@enron.com>','jason.williams@enron.com',s)
        #print ('37')
    if s.find( 'paul.e.ybarbo@worldnet.att.net')!=-1:
        s=re.sub('paul.e.ybarbo@worldnet.att.net','paul.ybarbo@enron.com',s)
        #print ('38')
    if s.find( 'chairman.ken@enron.com')!=-1:
        s=re.sub('chairman.ken@enron.com','kenneth.lay@enron.com',s)
        #print ('38')
    if s.find( 'skilling@enron.com')!=-1:
        s=re.sub('skilling@enron.com','jeff.skilling@enron.com',s)
    if s.find( 'jeffreyskilling@yahoo.com')!=-1:
        s=re.sub('jeffreyskilling@yahoo.com','jeff.skilling@enron.com',s)
    if s.find( 'skilling@enron.com')!=-1:
        s=re.sub('skilling@enron.com','jeff.skilling@enron.com',s)
    if s.find( 'jskilli@enron.com')!=-1:
        s=re.sub('jskilli@enron.com','jeff.skilling@enron.com',s)
        #print ('38')

    if s.find( 'hannon@enron.com')!=-1:
        s=re.sub('hannon@enron.com','kevin.hannon@enron.com',s)
    if s.find( 'kevin_a_howard.enronxgate.enron@enron.net')!=-1:
        s=re.sub('kevin_a_howard.enronxgate.enron@enron.net','a..howard@enron.com',s)
    if s.find( 'rex_shelby@enron.net')!=-1:
        s=re.sub('rex_shelby@enron.net','rex.shelby@enron.com',s)
    if s.find( 'f..calger@enron.com')!=-1:
        s=re.sub('f..calger@enron.com','christopher.calger@enron.com',s)
    if s.find( 'calger@enron.com')!=-1:
        s=re.sub('calger@enron.com','christopher.calger@enron.com',s)
    if s.find( 'rice@enron.com')!=-1:
        s=re.sub('rice@enron.com','ken.rice@enron.com',s)
    if s.find( 'ken_rice@enron.net')!=-1:
        s=re.sub('ken_rice@enron.net','ken.rice@enron.com',s)
    if s.find( 'lfastow@pop.pdq.net')!=-1:
        s=re.sub('lfastow@pop.pdq.net','lfastow@pdq.net',s)
  
    if s.find( 'dave.delainey@enron.com')!=-1:
        s=re.sub('dave.delainey@enron.com','david.delainey@enron.com',s)
    if s.find( 'delainey@enron.com')!=-1:
        s=re.sub('delainey@enron.com','david.delainey@enron.com',s)
 
    if s.find( 'lawrencelawyer@aol.com')!=-1:
        s=re.sub('lawrencelawyer@aol.com','larry.lawyer@enron.com',s)
 
    if s.find( 'belden@enron.com')!=-1:
        s=re.sub('belden@enron.com','tim.belden@enron.com',s)
    if s.find( 'belden@enron.com')!=-1:
        s=re.sub('lawrencelawyer@aol.com','tim.belden@enron.com',s)

    
    if s.find( 'jeffrey.a.shankman@enron.com')!=-1:
        s=re.sub('jeffrey.a.shankman@enron.com','jeffrey.shankman@enron.com',s)

    if s.find( 'colwell@enron.com')!=-1:
        s=re.sub('colwell@enron.com','wes.colwell@enron.com',s)
    return s
emails['From']=emails['From'].apply(last_to)
emails['To']=emails['To'].apply(last_to)


# def chose(name):
        # name=str(name)
        # if name in anrn_emp['email1'].tolist():
            # return True
        # else:
            # return False
def selectto(name):
        name=str(name)
        i = 0
        for s in anrn_emp['email1'].tolist():
             if name.find(s)!=-1:
                 i=i+1
        if i == 0 :
            return False
        else :
            return True



print('From-start:')
print(len(emails))
emails_u=emails[emails['From'].apply(selectto)]
emails_from=emails_u
print('from-done:')
print(len(emails_u))
emails_u=emails_u[emails_u['To'].apply(selectto)]
print('x-to-done:')
print(len(emails_u))



print(len(emails_u_drop))


emails_u.to_csv('output/emails_use.csv')
