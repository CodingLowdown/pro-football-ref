#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu Mar 26 08:23:39 2020

@author: nicholaslowe
"""
import requests
from bs4 import BeautifulSoup as bs
from bs4 import Comment as Comment
import numpy as np
import pandas as pd
import sqlite3
from multiprocessing import Pool
Base_URL="https://www.pro-football-reference.com"

TeamsURL=Base_URL+"/teams/"

headers=['urlteam','startYear','EndYear','TeamN']
vals=[['nwe',	'1960',	'1970',	'BOS']
,['nyj',	'1960',	'1962',	'NYT']
,['clt',	'1953',	'1983',	'BAL']
,['clt',	'1984',	'2019',	'IND']
,['oti',	'1960',	'1996',	'HOU']
,['oti',	'1997',	'2019',	'TEN']
,['kan',	'1960',	'1962',	'DTX']
,['sdg',	'1960',	'1960',	'LAC']
,['sdg',	'1961',	'2016',	'SDG']
,['sdg',	'2017',	'2019',	'LAC']
,['rai',	'1960',	'1981',	'OAK']
,['rai',	'1982',	'1994',	'RAI']
,['rai',	'1995',	'2019',	'OAK']
,['ram',	'1946',	'1994',	'RAM']
,['ram',	'1995',	'2015',	'STL']
,['ram',	'2016',	'2019',	'LAR']
,['crd',	'1950',	'1959',	'CRD']
,['crd',	'1960',	'1987',	'STL']
,['crd',	'1988',	'1993',	'PHO']
,['crd',	'1994',	'2019',	'ARI']
,['htx',	'1990',	'2019',	'HOU']
,['rav',	'1995',	'2019',	'BAL']
]
team_mapping=pd.DataFrame(vals, columns=headers)




session=requests.session() 

def uniquify(df_columns):
    seen = set()

    for item in df_columns:
        fudge = 1
        newitem = item

        while newitem in seen:
            fudge += 1
            newitem = "{}_{}".format(item, fudge)

        yield newitem
        seen.add(newitem)


def get_team_df(session):
    res1=session.get(TeamsURL)
    
    soup1 = bs(res1.text.encode('utf8'), 'html.parser')
    
    Colheaders=[]
    for heads in soup1.select("#teams_active thead")[0].find_all('th')[7:]:
            Colheaders.append(heads.text)
            
                              
    
    for thead in soup1.select("#teams_active thead"):
        tbody = thead.find_next_sibling("tbody")
        rows=tbody.find_all('tr')
        rownumselect=[]
        for rown in rows:
            try:
                rownumselect.append(rown.find('th',{'class':'left'}).find('a').get('href'))
            except:
                rownumselect.append('')
    
        table = "<table>%s</table>" % (str(thead) + str(tbody))
    
        df = pd.read_html(str(table))[0]
        df['href'] = rownumselect
    
    Final_active_df=df.loc[df['href'] != '']
    Final_active_df=Final_active_df.reset_index()
    Final_active_df=Final_active_df.drop(Final_active_df.columns[[0]], axis=1)
    return Final_active_df

def get_team_data(session,teamname):
    Ind_Team=Base_URL+teamname
    
    res2=session.get(Ind_Team)
    
    soup2 = bs(res2.text.encode('utf8'), 'html.parser')
    
    
    for thead in soup2.select("#team_index thead"):
            tbody = thead.find_next_sibling("tbody")
            rows=tbody.find_all('tr')
            rownumselect=[]
            for rown in rows:
                try:
                    rownumselect.append(rown.find('th',{'class':'left'}).find('a').get('href'))
                except:
                    rownumselect.append('')
        
            table = "<table>%s</table>" % (str(thead) + str(tbody))
        
            df = pd.read_html(str(table))[0]
            df['href'] = rownumselect
    Final_ind_active_df=df
    return Final_ind_active_df

def get_YearTeam_data(session,YearTeam):
    Ind_Year=Base_URL+YearTeam
    res3=session.get(Ind_Year)
    soup3 = bs(res3.text.encode('utf8'), 'html.parser')
    
    for thead in soup3.select("#games thead"):
        tbody = thead.find_next_sibling("tbody")
        rows=tbody.find_all('tr')
        rownumselect=[]
        for rown in rows:
            try:
                rownumselect.append(rown.find('td',{'data-stat':'boxscore_word'}).find('a').get('href'))
            except:
                rownumselect.append('')
        
        table = "<table>%s</table>" % (str(thead) + str(tbody))
        
        df = pd.read_html(str(table))[0]
        df['href'] = rownumselect
        
    
    Final_Year_active_df=df
    return Final_Year_active_df

def get_indGame_data(session,IndGame):
    Ind_Year=Base_URL+IndGame
    try:
        res4=session.get(Ind_Year)
    except:
        try:
            res4=session.get(Ind_Year)
        except:
            try:
                session=requests.session() 
                res4=session.get(Ind_Year)
            except:
                res4=''
                
    soup4 = bs(res4.text.encode('utf8'), 'html.parser')
    
    for thead in soup4.select("#player_offense thead"):
        tbody = thead.find_next_sibling("tbody")
        rows=tbody.find_all('tr')
        rownumselect=[]
        for rown in rows:
            try:
                rownumselect.append(rown.find('th',{'data-stat':'player'}).find('a').get('href'))
            except:
                rownumselect.append('')
        
        table = "<table>%s</table>" % (str(thead) + str(tbody))
        
        df = pd.read_html(str(table))[0]
        df['href'] = rownumselect
    playeroffense_DF=df
    tables=soup4.select('#all_team_stats')[0]
    ind=0
    while ind < len(tables.find_all(text=lambda text: isinstance(text, Comment))):
        comment = tables.find_all(text=lambda text: isinstance(text, Comment))[ind]
        table= bs(comment, 'html.parser')
        for thead in table.select("#team_stats thead"):
            tbody = thead.find_next_sibling("tbody")
            rows=tbody.find_all('tr')
            table = "<table>%s</table>" % (str(thead) + str(tbody))
            
            df = pd.read_html(str(table))[0]
        ind+=1
    teamstats_df=df
    tables=soup4.select('#all_game_info')[0]
    ind=0
    while ind < len(tables.find_all(text=lambda text: isinstance(text, Comment))):
        comment = tables.find_all(text=lambda text: isinstance(text, Comment))[ind]
        table= bs(comment, 'html.parser')
        for thead in table.select("table"):
            
            rows=tbody.find_all('tr')
            table = "<table>%s</table>" % (str(thead) )
            
            df = pd.read_html(str(table))[0]
            df.columns= df.iloc[0]
            df=df.drop(0)
        ind+=1
    gameinfo_df=df
    try:
        tables=soup4.select('#all_officials')[0]
        ind=0
        while ind < len(tables.find_all(text=lambda text: isinstance(text, Comment))):
            comment = tables.find_all(text=lambda text: isinstance(text, Comment))[ind]
            table= bs(comment, 'html.parser')
            for thead in table.select("table"):
                
                rows=tbody.find_all('tr')
                table = "<table>%s</table>" % (str(thead) )
                
                df = pd.read_html(str(table))[0]
                df.columns= df.iloc[0]
                df=df.drop(0)
            ind+=1
        officials_df=df
    except:
        officials_df="NA"
    try:
        tables=soup4.select('#all_home_starters')[0]
        ind=0
        while ind < len(tables.find_all(text=lambda text: isinstance(text, Comment))):
            comment = tables.find_all(text=lambda text: isinstance(text, Comment))[ind]
            table= bs(comment, 'html.parser')
            for thead in table.select("table"):
               
                rows=tbody.find_all('tr')
                table = "<table>%s</table>" % (str(thead))
                
                df = pd.read_html(str(table))[0]
                
            ind+=1
        homestarters_df=df
    except:
        homestarters_df="NA"
    try:
        tables=soup4.select('#all_vis_starters')[0]
        ind=0
        while ind < len(tables.find_all(text=lambda text: isinstance(text, Comment))):
            comment = tables.find_all(text=lambda text: isinstance(text, Comment))[ind]
            table= bs(comment, 'html.parser')
            for thead in table.select("table"):
                rows=tbody.find_all('tr')
                table = "<table>%s</table>" % (str(thead))
                
                df = pd.read_html(str(table))[0]
                
            ind+=1
        awaystarters_df=df
        result = pd.concat([homestarters_df, awaystarters_df],axis=0)
        playeroffense_DF.columns =playeroffense_DF.columns.droplevel(0)
        playeroffense_DF=pd.merge(playeroffense_DF, result, on='Player', how='left')
    except:
        playeroffense_DF['Pos']="N/A"
    
    
    
    return playeroffense_DF,teamstats_df,gameinfo_df,officials_df


def master(nums):
    Team_list=get_team_df(session)
    Teamdf=Team_list.iloc[[nums]]
    Team_Year_Data=get_team_data(session,Teamdf['href'].iloc[0])
    Team_Year_Data=Team_Year_Data.loc[Team_Year_Data['href'] != '']
    Team_Year_Data.columns =Team_Year_Data.columns.droplevel(0)
    Team_Year_Data['Year']=Team_Year_Data['Year'].astype(str)
    Team_Year_Data=Team_Year_Data[(Team_Year_Data['Year']>'1949')]
    
    Final_Player_table=[]
    teamstats_table=[]
    gameinfo_table=[]
    officials_table=[]
    for years in Team_Year_Data['']:
        Team_Year_Game_Data=get_YearTeam_data(session,years)
        Team_Year_Game_Data=Team_Year_Game_Data.loc[Team_Year_Game_Data['href'] != '']
        Team_Year_Game_Data['Year']=years.split("/")[:4][3].replace(".htm","")
        Team_Year_Game_Data['TeamURLName']=years.split("/")[:4][2]
        for ind,games in enumerate(Team_Year_Game_Data['href']):
            print(years+' '+games)
            df=Team_Year_Game_Data.iloc[[ind]]
            df.columns =df.columns.droplevel(0)
            df=df[df.columns.unique()] 
            res_dfs=get_indGame_data(session,games)
            df['key'] = 0
            joint=res_dfs[0]
            joint1=res_dfs[1]
            joint2=res_dfs[2]
            joint3=res_dfs[3]
            joint['key'] = 0
            joint1['key'] = 0
            joint2['key'] = 0
            joint=joint[joint.columns.unique()] 
            joint2=joint2[joint2.columns.unique()] 
            try:
                joint3['key'] = 0
                dfn=pd.merge(df,joint3,on='key',how='outer')
                dfn.columns=list(uniquify(dfn.columns))
                officials_table.append(dfn)
                
            except:
                'NA'
            
            dfn=pd.merge(df,joint,on='key',how='outer')
            dfn.columns=list(uniquify(dfn.columns))
            Final_Player_table.append(dfn)
            dfn=pd.merge(df,joint1,on='key',how='outer')
            dfn.columns=list(uniquify(dfn.columns))
            teamstats_table.append(dfn)
            dfn=pd.merge(df,joint2,on='key',how='outer')
            dfn.columns=list(uniquify(dfn.columns))
            gameinfo_table.append(dfn)


    Final_teamStats_append=[]
    for teami in teamstats_table:
        TabYear=list(teami['_2'])[0]
        TabTeam=list(teami['_3'])[0]
        try:
            TeamN=team_mapping[(team_mapping['startYear']<= TabYear) & (team_mapping['EndYear'] >= TabYear) & (team_mapping['urlteam'] == TabTeam)]['TeamN'].tolist()[0]
        except:
            TeamN=list(teami['_3'])[0].upper()
        headersforSTatstab=teami.columns.to_list()
        for ind,ij in enumerate(headersforSTatstab):
            
            if ij == TeamN:
                headersforSTatstab[ind]=TabTeam
                teami.columns = headersforSTatstab
        l=['_2','_3','Week',
           '']
        res=pd.pivot_table(teami,index=l,columns='Unnamed: 0',values=TabTeam,aggfunc='sum').reset_index()
        res=res.rename(columns = {'_2':'Year'})
        res=res.rename(columns = {'_3':'Team'})
        Final_teamStats_append.append(res)
    
    Final_teamStats_Table=pd.concat(Final_teamStats_append)
    Final_teamStats_Table = Final_teamStats_Table[Final_teamStats_append[0].columns]
    cardsOutput= Final_teamStats_Table[['Year','Team','Week','','Sacked-Yards']]
    cardsOutput["Sacked"] = cardsOutput["Sacked-Yards"].str.split('-',1).str[0]
    cardsOutput["Yards"] = cardsOutput["Sacked-Yards"].str.split('-',1).str[1]
    FinalCardsOutput=cardsOutput[['Year','Team','Week','','Sacked','Yards']]
    final_final_Final_Player_table=pd.concat(Final_Player_table)
    final_final_Final_Player_table = final_final_Final_Player_table[Final_Player_table[0].columns]
    conn = sqlite3.connect(':memory:')
    #write the tables
    final_final_Final_Player_table.to_sql('PlayerTab', conn, index=False)
    team_mapping.to_sql('team_mapping', conn, index=False)
    
    qry = '''
        select  
            pt.*,
            case when tm.urlteam IS NULL then lower(Tm_y) else tm.urlteam END as urlteam
        from
            PlayerTab as pt left join team_mapping as tm on
            _x_2>= startYear and _x_2<=EndYear 
            and TeamN=Tm_y
        '''
    QBStarts = pd.read_sql_query(qry, conn)
    QBStarts=QBStarts[QBStarts['Pos']=="QB"]
    QBStarts=QBStarts[['_x_2','urlteam','Week','_x','Player']]
    QBStarts=QBStarts.rename(columns = {'_x_2':'Year'})
    QBStarts=QBStarts.rename(columns = {'urlteam':'Team'})
    QBStarts=QBStarts.rename(columns = {'_x':''})
    FinalCardsOutput=pd.merge(FinalCardsOutput,QBStarts[['Team','','Player']],left_on=['Team',''], right_on = ['Team',''],how='left')
    FinalCardsOutput.to_csv('/Users/nicholaslowe/Desktop/code/David_N/dump_files/'+FinalCardsOutput['Team'].iloc[0]+'.csv')
    return FinalCardsOutput
    

#nums=0
restest=master(7)

#numteams=list(range(0,32,1))
numteams = np.arange(0,32,1)

p = Pool(32)
retest=p.map(master, numteams)
p.terminate()
p.join()

final_res=pd.concat(retest)
final_res['']=Base_URL+final_res['']

final_res.to_csv('/Users/nicholaslowe/Desktop/code/David_N/Sacks_Starters_32.csv')



/teams/det/1955.htm /boxscores/195512040chi.htm
/teams/was/1955.htm /boxscores/195510090was.htm
/teams/ram/1958.htm /boxscores/195811160gnb.htm
/teams/nyg/1957.htm /boxscores/195711100nyg.htm
/teams/phi/1958.htm /boxscores/195811090phi.htm
/teams/gnb/1960.htm /boxscores/196012260phi.htm
/teams/nwe/1960.htm /boxscores/196010230den.htm
/teams/cle/1950.htm /boxscores/195011120cle.htm

/teams/crd/1954.htm /boxscores/195410240crd.htm
KeyboardInterrupt
/teams/clt/1957.htm /boxscores/195710270clt.htm
/teams/sfo/1959.htm /boxscores/195910110gnb.htm
/teams/crd/1954.htm /boxscores/195410310crd.htm
/teams/det/1954.htm /boxscores/195409260det.htm
/teams/sfo/1959.htm /boxscores/195910180det.htm
/teams/clt/1957.htm /boxscores/195711030clt.htm
/teams/chi/1955.htm /boxscores/195509250clt.htm
/teams/crd/1954.htm /boxscores/195411070phi.htm
/teams/det/1954.htm /boxscores/195410100det.htm
/teams/sfo/1959.htm /boxscores/195910250sfo.htm
/teams/clt/1957.htm /boxscores/195711100was.htm
/teams/chi/1955.htm /boxscores/195510020gnb.htm


for nums in numteams:
    Teamdf=Team_list.iloc[[nums]]
    Team_Year_Data=get_team_data(session,Teamdf['href'].iloc[0])
    Team_Year_Data=Team_Year_Data.loc[Team_Year_Data['href'] != '']
    Team_Year_Data.columns =Team_Year_Data.columns.droplevel(0)
    Team_Year_Data['Year']=Team_Year_Data['Year'].astype(str)
    Team_Year_Data=Team_Year_Data[(Team_Year_Data['Year']>'1949')]








test.columns=list(uniquify(test.columns))
test2.columns=list(uniquify(test2.columns))

Final_Player_table[0].columns.is_unique

df = Final_Player_table[0][Final_Player_table[0].columns.unique()] 

pd.concat(Final_Player_table)
pd.concat(Final_Player_table[200:203],axis=0, ignore_index=False).fillna(0)


test=Final_Player_table[200]
test2=Final_Player_table[201]
test.columns =test.columns.droplevel(0)
test=test[test.columns.unique()] 
test2.columns =test2.columns.droplevel(0)
test2=test2[test2.columns.unique()] 

pd.concat([test,test2],axis=0, ignore_index=False)



final_final_Final_Player_table=pd.concat(Final_Player_table)
final_final_Final_Player_table = final_final_Final_Player_table[Final_Player_table[0].columns]
final_final_officials_table=pd.concat(officials_table)
final_final_officials_table = final_final_officials_table[officials_table[0].columns]



final_final_teamstats_table=pd.concat(teamstats_table)








FinalCardsOutput.to_csv('TestCardsSacks.csv')







test1=teami





test1['_2']


final_final_teamstats_table = final_final_teamstats_table[teamstats_table[0].columns]




final_final_gameinfo_table=pd.concat(gameinfo_table)
final_final_gameinfo_table = final_final_gameinfo_table[gameinfo_table[0].columns]

final_final_Final_Player_table
final_final_Final_Player_table=final_final_Final_Player_table.loc[final_final_Final_Player_table['_y'] != '']




res=pd.pivot_table(final_final_teamstats_table,index=l,columns='Game Info',values='Game Info_2',aggfunc='sum').reset_index()






l=list(final_final_gameinfo_table.columns)
l.remove('Game Info')
l.remove('Game Info_2')
l=[
 '']
res=pd.pivot_table(final_final_gameinfo_table,index=l,columns='Game Info',values='Game Info_2',aggfunc='sum').reset_index()



final_final_Final_Player_table.to_csv('PlayerTable_crds.csv')
final_final_officials_table.to_csv('officials.csv')
final_final_teamstats_table.to_csv('teamstats.csv')
final_final_gameinfo_table.to_csv('gameinfo.csv')

nums=0

Team_list['href'][0]

results=get_indGame_data(session,Final_Year_active_df['href'][0])

results.append(get_indGame_data(session,Final_Year_active_df['href'][1]))




os.getcwd()







