# python3 bet.py

from collections import defaultdict
from bs4 import BeautifulSoup
import functools as ft
import numpy as np
import os
import pandas as pd
import random
import requests
import sys
import time
import uuid

# set up
random.seed(1)
root_link = "https://fbref.com/"
current_season = "2022"
root_dir = os.getcwd()
output_dir = str(os.path.join(root_dir, "betdata"))
data_dir, test_dir = str(os.path.join(output_dir, "FINAL")), str(os.path.join(output_dir, "test"))
if not os.path.exists(output_dir):  os.makedirs(output_dir)
if not os.path.exists(data_dir):  os.makedirs(data_dir)
if not os.path.exists(test_dir):  os.makedirs(test_dir)
s_for, s_vs, s_2000 = "for" , "vs", "20"
s_tk, s_null = "SQUADS_STATS", "NULL"
s_sep = "\t|\t"
req_header = { # generic header
  "Accept-Language": "en-US", "Content-Type": "text/html", 
  "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:101.0) Gecko/20100101 Firefox/101.0", 
  }
comps = {

  # NORTH AMERICA
  "American Major League": "en/comps/22/history/Major-League-Soccer-Seasons/", 
  # EUROPE
  "Dutch Eredivisie": "en/comps/23/history/Eredivisie-Seasons/", 
  "English Premier League": "en/comps/9/history/Premier-League-Seasons/", 
  "French Ligue 1": "en/comps/13/history/Ligue-1-Seasons/", 
  "German Bundesliga": "en/comps/20/history/Bundesliga-Seasons/", 
  "Italian Serie A": "en/comps/11/history/Serie-A-Seasons/", 
  "Spanish La Liga": "en/comps/12/history/La-Liga-Seasons/", 
  "Portuguese Primeira Liga": "en/comps/32/history/Primeira-Liga-Seasons/", 
  # ASIA
  "Indian Super League": "en/comps/82/history/Indian-Super-League-Seasons/", 
  "Japanese J1 League": "en/comps/25/history/J1-League-Seasons/", 
  "South Korean K League 1": "en/comps/55/history/K-League-1-Seasons/", 

  }

def is_empty_dict(d):  return (d == {})

def list_eq(l1, l2):
  if len(l1) != len(l2):  return False
  for i in range(len(l1)):
    if l1[i] != l2[i]:  return False
  return True

def chk_cols(c):
  cns = []
  for i in c:
    cn = None
    if type(i) == tuple: j, k = i[0], i[1]
    if type(i) == str: j, k = i, None
    ix = j.find("level_0") # null header indicator
    if ix > 0 and k is None:  cn = (j[ix+8:]).replace(" ", "_")
    if ix < 0 and k is None:  cn = j.replace(" ", "_")
    if ix > 0 and k is not None:  cn = k.replace(" ", "_")
    if ix < 0 and k is not None:  cn = j.replace(" ", "_") + "---" + k.replace(" ", "_")
    cns.append(cn)
  return cns

def get_comp_teams_stats(leagues, num_seasons, log):
  compslinks, dfs = defaultdict(list), {}
  ret = {}
  for k, c in comps.items():
    if k in leagues:
      rt = np.random.normal(delay_mean, delay_var, 1)[0]
      time.sleep(rt)
      req = requests.get(root_link + c)
      time.sleep(rt)
      soup = BeautifulSoup(req.content, "html.parser")
      for i in soup.find_all("table"):
        for j in i.find_all("a"):
          jj = str(j)
          js = jj.find("/en/comps")
          je = jj.find("Stats")
          if js > 0 and je > 0:  compslinks[c].append(jj[js:je+5])
  for comp, links in compslinks.items():
    tcomp = comp[comp[:-1].rfind("/")+1:-1]
    dfs[tcomp] = {}
    folder = output_dir + "/" + tcomp
    if log and not os.path.exists(folder):  os.makedirs(folder)
    # avoid empty first page during preseasons
    all = [root_link[:-1] + l for l in links][2:num_seasons*2+1:2]
    for link in all: # duplicates
      u = (uuid.uuid4()).hex
      ix = link[:-1].rfind("/")
      tlink1, tlink2 = link[ix+1:ix+5], link[ix+6:ix+10]
      tlink = f"xxx_{u}"
      if s_2000 in tlink1 and s_2000 in tlink2:  tlink = tlink2
      if s_2000 in tlink1 and s_2000 not in tlink2:  tlink = tlink1
      if s_2000 not in tlink1 and s_2000 not in tlink2:  tlink = current_season
      if tlink in dfs[tcomp]:  pass
      else:
        rt = np.random.normal(delay_mean, delay_var, 1)[0]
        time.sleep(rt)
        dfs_season = pd.read_html(link, keep_default_na=True)
        time.sleep(rt)
        dfs[tcomp][tlink] = {}
        print("\n\n\n", tcomp, s_sep, link, s_sep, tlink)
        for d in dfs_season:  d.columns = [ "_".join(i) for i in d.columns ]
        dfs[tcomp][tlink][s_tk] = dfs_season
        time.sleep(rt)
        pdfs = get_comp_teams_players_stats(link)
        time.sleep(rt)
        for pdfi, pdfj in pdfs.items():
          dfs[tcomp][tlink][pdfi.upper()] = pdfj
  if log:
    uu = (uuid.uuid4()).hex
    s = f"xxx_{uu}"
    for ii, jj in dfs.items():
      ret[ii] = {}
      uuu = (uuid.uuid4()).hex
      for kk in jj.items():
        for ll in kk:
          if type(ll) == str and s_2000 in ll:  s = ll
          if type(ll) == dict:
            ret[ii][s] = {}
            for mm, nn in ll.items():
              folder = os.path.join(output_dir, f"{ii}_{uuu}", s)
              if not os.path.exists(folder):  os.makedirs(folder)
              squad_dfs, player_dfs, match_dfs = [], [], []
              ret[ii][s][mm] = {}
              for oo in nn:
                oo.columns = chk_cols(list(oo.columns.values))
                cols = [ i if "/" not in i else i.replace("/", "") for i in list(oo.columns.values) ]
                if mm == s_tk and "Squad" in cols:
                  oo.set_index("Squad")
                  teams = list(oo["Squad"])
                  if "vs" in teams[0]:
                    oo["Squad"] = list(map(lambda s: s[3:], teams))
                    oo.columns = [ f"vs_{i}" if i != "Squad" else i for i in cols ]
                  oo["Squad"] = list(map(lambda x: str(str(x).encode('unicode-escape').decode('utf-8')), oo["Squad"]))
                  oo["Season"], oo["League"] = [s] * len(oo["Squad"]), [ii[:-8]] * len(oo["Squad"])
                  squad_dfs.append(oo)
                elif mm != s_tk and "Player" in cols:
                  oo.set_index("Player")
                  oo["Player"] = list(map(lambda x: str(str(x).encode('unicode-escape').decode('utf-8')), oo["Player"]))
                  oo["Season"], oo["League"], oo["Squad"] = [s] * len(oo["Player"]), [ii[:-8]] * len(oo["Player"]), \
                    [mm] * len(oo["Player"])
                  player_dfs.append(oo)
                elif mm != s_tk and "Date" in cols and "Round" in cols and "Result" in cols:
                  oo.set_index("Round")
                  oo["Opponent"] = list(map(lambda x: (str(str(x).encode('unicode-escape').decode('utf-8')).upper()).replace(" ", "_"), \
                    oo["Opponent"]))
                  oo["Captain"] = list(map(lambda x: str(str(x).encode('unicode-escape').decode('utf-8')), oo["Captain"]))
                  oo["Referee"] = list(map(lambda x: str(str(x).encode('unicode-escape').decode('utf-8')), oo["Referee"]))
                  oo["Formation"] = [ str(i).replace("-", ",") for i in oo["Formation"] ]
                  oo["Season"], oo["League"], oo["Squad"] = [s] * len(oo["Round"]), [ii[:-8]] * len(oo["Round"]), [mm] * len(oo["Round"])
                  match_dfs.append(oo)
              squad_df = ft.reduce(lambda l, r: pd.merge(l, r, on="Squad", how="left", copy=False), squad_dfs) if len(squad_dfs) > 0 \
                else None
              player_df = ft.reduce(lambda l, r: pd.merge(l, r, on="Player", how="left", copy=False), player_dfs) if len(player_dfs) > 0 \
                else None
              match_df = match_dfs[0] if len(match_dfs) == 1 else None
              if squad_df is not None:
                squad_df = squad_df.T.drop_duplicates().T
                squad_df.to_csv(os.path.join(output_dir, f"{ii}_{uuu}", s, f"squads-{s}.csv"))
                ret[ii][s][mm]["Squads"] = squad_df
              if player_df is not None:
                # player_df.columns = [ i[:-2] if "_x" in i or "_y" in i else i for i in list(player_df.columns.values) ]
                player_df = player_df.T.drop_duplicates().T
                if "Matches_x" in list(player_df.columns.values):  player_df.drop("Matches_x", axis=1, inplace=True)
                if "Matches_y" in list(player_df.columns.values):  player_df.drop("Matches_y", axis=1, inplace=True)
                player_df.to_csv(os.path.join(output_dir, f"{ii}_{uuu}", s, f"players-{mm}-{s}.csv"))
                ret[ii][s][mm]["Players"] = player_df
              if match_df is not None:
                if "Match_Report" in list(match_df.columns.values):  match_df.drop("Match_Report", axis=1, inplace=True)
                match_df.to_csv(os.path.join(output_dir, f"{ii}_{uuu}", s, f"matches-{mm}-{s}.csv"))
                ret[ii][s][mm]["Matches"] = match_df
  # output
  return ret

def get_comp_teams_players_stats(slink):
  x = []
  dfs = {}
  req = requests.get(slink)
  soup = BeautifulSoup(req.content, "html.parser")
  for i in soup.find_all("table"):
    for j in i.find_all("a"):
      jj = str(j)
      if "/en/squads" in jj:  x.append(jj)
  xx = [ root_link + i[i.find("/en/squads"):i.find("Stats")+5] for i in x ]
  for i in list(set(xx)):
    rt = np.random.normal(15, 2, 1)[0]
    j = i[i[:-1].rfind("/")+1:i.find("Stats")-1]
    j = j.replace("-", "_")
    time.sleep(rt)
    dfs[j] = pd.read_html(i, keep_default_na=True)
    time.sleep(rt)
  return dfs

def run_and_time(k):
  st = time.perf_counter()
  x = get_comp_teams_stats([k], season_count, True)
  et = time.perf_counter()
  t = round((et - st), 3)
  print(t)
  return x

def merge_csvs():
  all, rmv, rmvdir = [], [], []
  for root, dirs, files in os.walk(output_dir):
    for f in files:
      if f.endswith(".csv"):  all.append(str(os.path.join(root, f)))
  all_matches, all_players, all_squads = \
    [ i for i in all if "matches-" in i ], [ i for i in all if "players-" in i ], [ i for i in all if "squads-" in i ]
  dfs_matches, dfs_players, dfs_squads = \
    map(pd.read_csv, all_matches), map(pd.read_csv, all_players), map(pd.read_csv, all_squads)
  matches, players, squads = \
    map(lambda x: x.drop("Unnamed: 0", axis=1), dfs_matches), \
    map(lambda x: x.drop("Unnamed: 0", axis=1), dfs_players), \
    map(lambda x: x.drop("Unnamed: 0", axis=1), dfs_squads)
  matches, players, squads = pd.concat(matches), pd.concat(players), pd.concat(squads)
  lg = matches['League'].head(1).values[0]
  matches.to_csv(os.path.join(output_dir, "FINAL", f"{lg}_matches.csv"), index=False)
  players.to_csv(os.path.join(output_dir, "FINAL", f"{lg}_players.csv"), index=False)
  squads.to_csv(os.path.join(output_dir, "FINAL", f"{lg}_squads.csv"), index=False)
  for root, dirs, files in os.walk(output_dir):
    for f in files:
      if f.endswith(".csv"):  rmv.append(str(os.path.join(root, f)))
  rmv = [ i for i in rmv if "\\test\\" not in i and "\\FINAL\\" not in i ]
  # cleanup
  # for i in rmv:  os.remove(i)
  # for root, dirs, files in os.walk(output_dir):
  #   for d in dirs:
  #     if d not in data_dir and d not in test_dir:  rmvdir.append(str(os.path.join(root, d)))
  # for rd in rmvdir:
  #   for root, dirs, files in os.walk(rd):
  #     for d in dirs:  os.rmdir(str(os.path.join(root, d)))
  #   if os.path.exists(rd):  os.rmdir(rd)
  return [ matches, players, squads ]


# scraping args
delay_mean, delay_var = 19, 4
season_count = 1

if __name__ == "__main__":
  print(season_count)
  print("done.")

