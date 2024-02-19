#!/bin/python3
#REQUIREMENTS: pip install ytmusicapi yt-dlp sanitize_filename sty music_tag
#requires apt install ffmpeg
#run ytmusicapi oauth to get oauth.json
#version 0.9

import json, sys, os, glob, subprocess, time, random, datetime, argparse, re, music_tag
from sanitize_filename import sanitize
from sty import fg, rs
from ytmusicapi import YTMusic
from difflib import SequenceMatcher

def dump_json(json, filename="temp.json"): # dump json_object as pretty json to file
  with open(filename, "w") as f:
    f.write(json.dumps(json, indent=2))

def sane_fn(f): #sanitize filename of illegal characters
  return sanitize(f.replace('/','-').replace('`',"'"))

def similar(a, b): #determine similarity of two strings, float 0 to 1
  return float(SequenceMatcher(None, a, b).ratio())
  
def delay(s=10): #delay plus or minus 50%
  a = int(s/2)
  b = int(s*1.5)
  time.sleep(random.randint(a,b))

def prompt_albums(r): # Propmt user which albums should be skipped.
  # Print numbered list
  # Save album number to indexed_id with browseId
  # Parse input to list of numbers to skip_list
  # Loop through albums again and pop ones where browseId matches associated number in skip_list
  print("Which albums should be skipped?")
  
  indexed_id = []
  to_skip = []

  num_albums = 1
  for a in r:
    album_id = a["browseId"]
    album_title = a["title"]
    print(f"  {num_albums:2d} - {album_title}")
    indexed_id.insert(num_albums, album_id) # ensure list is in order, I don't trust python loops to be ordered
    num_albums += 1
  
  print(f"Enter numbers to skip seperated by spaces ({1}-{num_albums-1}):")
  print(f"(Enter 0 or leave blank to not skip any albums.)")
  skip_str = str(input())
  skip_nums = re.compile('-?\d+').findall(skip_str)

  if len(skip_nums) == 0 or int(skip_nums[0]) == 0:
    print("Not skipping any albums...")
    return r

  # sanity check the skip list.
  for n in skip_nums:
    if int(n) < 1 or int(n) > num_albums-1:
      print(f"STOP == Invalid album number: {n}")
      sys.exit()

  # confirm skipped albums and add to to_skip list
  skip_str = ""
  for i in skip_nums:
    to_skip.append(indexed_id[i-1])
    skip_str += " " + str(i)

  print(f"Skipping {len(to_skip)} albums numbered:{skip_str}...")

  out_albums = []
  for a in r:
    if not (a["browseId"] in to_skip):
      out_albums.append(a)

  # print("New album list: "+ str(out_albums))
  return out_albums

def set_metadata( album, track, filename ): # runs after file is saved
  try:
    tags = music_tag.load_file(filename)
  except NotImplementedError:
    return
  if tags["album"]: 
    return #already done

  errors = 0
  tags["album"] = album["title"] # don't need to check if album["title"] exists, already used for folder name
  if "year" in album:
    tags["year"] = album["year"]
  else:
    errors += 1
  
  if "artists" in track and len(track["artists"]) > 0 and "name" in track["artists"][0]:
    tags["artist"] = track["artists"][0]["name"]
  else:
    errors += 1
  
  if "title" in track:
    tags["tracktitle"] = track["title"]
  else:
    errors += 1
  
  if "trackNumber" in track:
    tags["tracknumber"] = track["trackNumber"]
  else:
    errors += 1

  # ~ print("save metadata",tags["album"])
  tags.save()
  print(" -- got metadata", end="")

  if errors != 0:
    print("Metadata Incomplete")

def glob_exists(filename): #detect song with any extension, return filename
  fn = glob.glob(filename+".*")
  if fn:
    return fn[0]
  else:
    return False
  
  
#=====main()
start = time.time()
parser = argparse.ArgumentParser(description='Download complete discographies from youtube music')
parser.add_argument('artists', metavar='ARTIST', type=str, nargs='*', help='artist to download')
parser.add_argument('-f', '--file', metavar='FILE', type=str, default='', help='load list of artists from file, one artist per line')
parser.add_argument('-o', '--output-dir', metavar='PATH', type=str, default='music', help='store discographies in specified directory')
parser.add_argument('-s', '--skip-albums', action='store_true', help='prompt which albums to skip')
parser.add_argument('-t', '--skip-tags', action='store_true', help=' skip saving music tags')
parser.add_argument('-d', '--delay', action='store_true', help='delay ~40s per album to avoid google ban')
parser.add_argument('-l', '--live', action='store_true', help='include live albums') #===== DEBUG not implemented

parser.add_argument('--rescan', action='store_true', help='rescan all artists for missing metadata, newly avail songs')

args = parser.parse_args()

if args.output_dir[-1] == "/":
  args.output_dir = args.output_dir[0:-1]

if args.rescan:
  artists = os.listdir(args.output_dir)
else:
  if args.file: # require file or artist(s)
    with open(args.file, "r") as f:
      artists = f.read().split("\n")
  elif args.artists:
    artists = args.artists
  else:
    print("ERROR: it is required to have at least one artist or a --file artist list")
    sys.exit()
# refer to args.live, args.delay directly

if not os.path.exists("oauth.json"):
  print("cannot find oauth.json.  Please run\nytmusicapi oauth\non the command line to generate the file.")
  sys.exit()


def grab_discography(search):
  global c, ca, cur_artist, tot_artist, ytm
  r = ytm.search(search, filter="artists")
  artist = r[0]["artist"]
  cur_artist = cur_artist + 1

  d = similar(search.lower(), artist.lower())
  e = similar(("the "+search).lower(), artist.lower())
  if d < 0.9 and e < 0.9: #not a good match ===== DEBUG need to log this!
    print(f'best fit for "{search}" is "{artist}": not good enough to continue')
    return

  artist_id = r[0]["browseId"]
  artist_dir = sane_fn(artist)
  r = ytm.get_artist(artist_id)
  discography_id = r["albums"]["browseId"]
  try:
    discography_params = r["albums"]["params"]
    r = ytm.get_artist_albums(discography_id, discography_params)
  except:
    r = r['albums']['results'] #type 2 discography

  if args.skip_albums:
    r = prompt_albums(r)
  
  errors = 0
  b = 0
  num_albums = len(r)

  for a in r: #for each album in dicography
    ca = ca + 1
    album_id = a["browseId"]
    album_title = a["title"]
    album_dir = sane_fn(album_title)
    path = f"{args.output_dir}/{artist_dir}/{album_dir}/"
    os.makedirs(os.path.dirname(path), exist_ok=True)
    b = b + 1
    print(f"{cur_artist}/{tot_artist}: {artist} -- {b}/{num_albums}: {album_title}")
   
    s = ytm.get_album(album_id)
    delay(1)
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
      song_file = f"{n+1} - {song_sane}"

      f_old = glob_exists(f"{path}{n} - {song_sane}") # ===== TEMP for renaming
      if f_old:                                       #
        ext = f_old.split(".")[-1]                    #
        os.rename(f_old, f"{path}{song_file}.{ext}")  #
        out = f"RENAMED {path}{song_file}.{ext}"      #

      if song_id:
        song_filename = f"{path}/{song_file}"
        if glob_exists(song_filename):
          print(f"{fg.li_blue}SKIPPED{fg.rs}", end="")
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
            print(f"{fg.red}FAIL{fg.rs} - {song_file}", end="")
          else:  
            print(f"{fg.green}GOOD{fg.rs} - {song_file}", end="")
            c = c + 1
          if errors == 3:
            print("STOP == too many errors!")
            sys.exit()
          if args.delay:
            delay(40)
            
        if not args.skip_tags:
          fn = glob_exists(song_filename)
          if fn:
            set_metadata(a, t, fn)
        print("") # send newline
  
      else: #song_id = Null
        c = c + 1
        print(f"{fg.red}NULL{fg.rs} - {song_file}")

ytm = YTMusic("oauth.json") #===== DEBUG need to except for bad oauth
c = 0 # track count total
ca = 0 # album count total
cur_artist = 0
tot_artist = len(artists)
for artist in artists:
  if artist:
    grab_discography(artist)
        
# print end stats
end = time.time()
elapsed = int(end - start)
hms = str(datetime.timedelta(seconds=(elapsed)))
per_hour = int((3600 / elapsed) * c) if elapsed != 0 else 0
print(f"=== {fg.li_blue}DONE{fg.rs} {ca} albums; {c} songs in {hms}; {per_hour} songs/hour")
