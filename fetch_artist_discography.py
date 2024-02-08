#!/bin/python3
#REQUIREMENTS: pip install ytmusicapi yt-dlp sanitize_filename sty
#requires apt install ffmpeg
#run ytmusicapi oauth to get oauth.json
#version 0.4

import json, sys, os, subprocess, time, random, datetime, argparse
from sanitize_filename import sanitize
from sty import fg, rs
from ytmusicapi import YTMusic
from difflib import SequenceMatcher

def dump_json(r): #dump pretty json to file
  o = json.dumps(r, indent=2)
  with open("temp.json", "w") as f:
    f.write(o)

def sane_fn(f): #sanitize filename
  return sanitize(f.replace('/','-').replace('`',"'"))

def similar(a, b):
  return float(SequenceMatcher(None, a, b).ratio())
  
def delay(s=10): #delay plus or minus 50%
  a = int(s/2)
  b = int(s*1.5)
  time.sleep(random.randint(a,b))

#=====main()
start = time.time()
parser = argparse.ArgumentParser(description='Download complete discographies from youtube music')
parser.add_argument('artists', metavar='ARTIST', type=str, nargs='*', help='artist to download')
# ~ parser.add_argument('artist name', metavar='ARTIST', type=str, nargs='+', help='artist to download')
parser.add_argument('-f', '--file', metavar='FILE', type=str, default='', help='load list of artists from file, one artist per line')
parser.add_argument('-o', '--output-dir', metavar='PATH', type=str, default='music', help='store discographies in specified directory')
parser.add_argument('--live', action='store_true', help='include live albums')
args = parser.parse_args()

if args.file:
  with open(args.file, "r") as f:
    artists = f.read().split("\n")
elif args.artists:
  artists = args.artists
else:
  print("ERROR: it is required to have at least one artist or a --file artist list")
  sys.exit()
if args.output_dir[-1] == "/":
  args.output_dir = args.output_dir[0:-1]
if args.live:
  pass #=====DEBUG implement
  
if not os.path.exists("oauth.json"):
  print("cannot find oauth.json.  Please run\nytmusicapi oauth\non the command line to generate the file.")
  sys.exit()

def grab_discography(search):
  global c, ca, ytm
  r = ytm.search(search, filter="artists")
  artist = r[0]["artist"]

  d = similar(search.lower(), artist.lower())
  e = similar(("the "+search).lower(), artist.lower())
  # ~ print ("d",d,"; e",e)
  if d < 0.9 and e < 0.9: #not a good match
    print(f'best fit for "{search}" is "{artist}": not good enough to continue')
    return

  artist_id = r[0]["browseId"]
  artist_dir = sane_fn(artist)
  print ("===",artist)

  r = ytm.get_artist(artist_id)
  discography_id = r["albums"]["browseId"]
  try:
    discography_params = r["albums"]["params"]
    r = ytm.get_artist_albums(discography_id, discography_params)
  except:
    r = r['albums']['results'] #type 2 discography

  num_albums = len(r)
  errors = 0
  b = 0
  for a in r: #for each album in dicography
    ca = ca + 1
    album_id = a["browseId"]
    album_title = a["title"]
    album_dir = sane_fn(album_title)
    path = f"{args.output_dir}/{artist_dir}/{album_dir}/"
    os.makedirs(os.path.dirname(path), exist_ok=True)
    b = b + 1
    print(f"{b}/{num_albums} -- {album_title}")
   
    s = ytm.get_album(album_id)
    # ~ try: # ===== trying to identify live albums
      # ~ print(s["description"])
    # ~ except:
      # ~ pass
    tracks = len(s["tracks"])
    for n in range(tracks): #for each track in album
      t = s["tracks"][n]
      song_title = t["title"]
      song_id = t["videoId"]
      song_sane = sane_fn(song_title)
      song_file = f"{n} - {song_sane}"
      if song_id:
        song_filename = f"{path}/{song_file}"
        if os.path.exists(song_filename+".opus") or os.path.exists(song_filename+".m4a"):
          print(f"{fg.li_blue}SKIPPED{fg.rs}")
        else:
          cmd = f'yt-dlp -q -x -P "{path}" -o "{song_file}" -- {song_id} '
          try:
            u = subprocess.check_call(cmd, shell=True)
          except:
            print(cmd) #uncaught error
            dump_json(s)
            sys.exit()

          if u != 0: #result code
            errors = errors + 1
            print(f"{fg.red}FAIL{fg.rs} - {song_file}")
          else:  
            print(f"{fg.green}GOOD{fg.rs} - {song_file}")
            c = c + 1
          if errors == 3:
            print ("STOP == too many errors!")
            sys.exit()
          delay(2)
      else: #song_id = Null
        c = c + 1
        print(f"{fg.red}NULL{fg.rs} - {song_file}")

ytm = YTMusic("oauth.json")
c = 0
ca = 0
for artist in artists:
  if artist:
    grab_discography(artist)
        
end = time.time()
elapsed = int(end - start)
hms = str(datetime.timedelta(seconds=(elapsed)))
per_hour = int ((3600 / elapsed) * c)
print(f"=== {fg.li_blue}DONE{fg.rs} {ca} albums; {c} songs in {hms}; {per_hour} songs/hour")
