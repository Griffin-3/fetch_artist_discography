#!/bin/python3
#REQUIREMENTS: pip install ytmusicapi, yt-dlp, sanitize_filename, sty

import json, sys, os, subprocess, time, random, datetime
from sanitize_filename import sanitize
from sty import fg, rs
from ytmusicapi import YTMusic
ytm = YTMusic("oauth.json")

start = time.time()
def dumpr(r): #dump pretty json to file
  o = json.dumps(r, indent=2)
  with open("temp.json", "w") as f:
    f.write(o)

def sane_fn(f): #sanitize filenmae
  return sanitize(f.replace('/','-'))
  
def delay(s=10): #delay plus or minus 50%
  a = int(s/2)
  b = int(s*1.5)
  time.sleep(random.randint(a,b))

if len(sys.argv) == 1:
  print(f'USAGE: python3 {sys.argv[0]} "artist"')
  sys.exit()

r = ytm.search(sys.argv[1], filter="artists")
o = json.dumps(r, indent=2)

#if a close enought to artist ===== DEBUG
artist = r[0]["artist"]
artist_id = r[0]["browseId"]
artist_dir = sane_fn(artist)
print ("===",artist)

r = ytm.get_artist(artist_id)
discography_id = r["albums"]["browseId"]
discography_params = r["albums"]["params"]

r = ytm.get_artist_albums(discography_id, discography_params)
errors = 0
b = 0
c = 0
for a in r: #for each album in dicography
  album_id = a["browseId"]
  album_title = a["title"]
  album_dir = sane_fn(album_title)
  path = f"music/{artist_dir}/{album_dir}/"
  os.makedirs(os.path.dirname(path), exist_ok=True)
  b = b + 1
  print(b, '--', album_title)
  
  s = ytm.get_album(album_id)
  tracks = s["trackCount"]
  for n in range(tracks): #for each track in album
    t = s["tracks"][n]
    song_title = t["title"]
    song_id = t["videoId"]
    song_sane = sane_fn(song_title)
    song_file = f"{n} - {song_sane}"
    song_filename = f"{path}/{song_file}"
    if os.path.exists(song_filename+".opus"):
      print(f"{fg.li_blue}SKIPPED{fg.rs}")
    else:
      cmd = f'yt-dlp -q -x -P "{path}" -o "{song_file}" -- {song_id} '
      u = subprocess.check_call(cmd, shell=True)
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

end = time.time()
elapsed = int(end - start)
hms = str(datetime.timedelta(seconds=(elapsed)))
per_hour = int ((3600 / elapsed) * c)
print(f"=== {fg.li_blue}DONE{fg.rs} {len(r)} albums; {c} songs in {hms}; {per_hour} songs/hour")
