#!/bin/python3
#REQUIREMENTS: pip install ytmusicapi yt-dlp sanitize_filename sty music_tag
#requires apt install ffmpeg
#run ytmusicapi oauth to get oauth.json
#version 0.11

import json, sys, os, glob, subprocess, time, random, datetime, argparse, re, music_tag, sqlite3
from sanitize_filename import sanitize
from sty import fg, rs
from ytmusicapi import YTMusic
from difflib import SequenceMatcher

def dump_json(json, filename="temp.json"): # dump json_object as pretty json to file
  with open(filename, "w") as f:
    f.write(json.dumps(json, indent=2))

def sane_fn(f): #sanitize filename of illegal characters
  return sanitize(f.replace('/','-').replace('`',"'"))

def similar(a, b): # Determine similarity of two strings, float 0 to 1
  return float(SequenceMatcher(None, a, b).ratio())
  
def delay(s=10): # Delay plus or minus 50%
  a = int(s/2)
  b = int(s*1.5)
  time.sleep(random.randint(a,b))
  
def prompt_albums(r): # Prompt user which albums should be skipped.
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

def set_metadata(album, track, filename): # Runs after file is saved, return successful as bool
  try:
    tags = music_tag.load_file(filename)
  except NotImplementedError:
    return False
  if tags["album"]: 
    return True #already done

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
    return False
  return True

def glob_exists(filename): # Detect song with any extension, return filename
  fn = glob.glob(glob.escape(filename)+".*")
  if fn:
    return fn[0]
  else:
    return False
  
def is_live_album(album_name): # Determines from title if album is live
  return re.search(r"([\[\(]live[\]\)]|live (at|in|from|fm|bootleg|[1-2][0-9][0-9][0-9]|- )|^live! | live$| live!|fm broadcast)", album_name, re.I)

def write_error(text): # Append error to error.log, also db =====TODO
  with open("error.log", "a") as f:
    f.write(text+"\n")

def open_database(): # Create if necessary
  try:
    db = sqlite3.connect("discography.sq3")
  except sqlite3.Error as e:
    sys.exit(e)
  x = db.execute("SELECT EXISTS(SELECT 1 FROM sqlite_master WHERE type='table' AND name='artists');").fetchone()[0]
  if not x:
    db.execute("CREATE TABLE 'artists' ( 'id' INTEGER, 'artist' TEXT, 'status' INTEGER, PRIMARY KEY('id'))")
    db.execute("CREATE TABLE 'albums' ( 'id' INTEGER, 'artist_id' INTEGER, 'album' TEXT, 'status' INTEGER, PRIMARY KEY('id'))")
    db.execute("CREATE TABLE 'tracks' ( 'id' INTEGER, 'album_id' INTEGER, 'track' TEXT, 'status' INTEGER, PRIMARY KEY('id'))")
    db.execute("CREATE TABLE 'errors' ( 'code' TEXT, 'message' TEXT)")
    db.commit()
    args.rescan = True
  return db

def db_fetch(sql, values): # Get db result only, single row or scalar
  global db
  if type(values) is str:
    values = (values,) #needs to be tuple
  r = db.execute(sql, values).fetchone()
  if r and len(r)==1:
    return r[0]
  return r

status = {'PRELOAD': 1, 'IGNORED': 2, 'LIVE': 3, 'NOMETADATA': 4, 'INCOMPLETE': 5, 'FINISHED': 9}
statout= {1: 'PRELOAD', 2: 'IGNORED', 3: 'LIVE', 4: 'NOMETADATA', 5: 'INCOMPLETE', 9: 'FINISHED'}
def db_check_status(artist="", album="", track="", parentid=""): # Check status, insert value if needed. Returns id or 0 if finished
  global db, status
  global cur_artist, tot_artist, cur_album, num_albums, artist_sane
  if artist:
    val = (artist,)
    sql = "SELECT status,id FROM artists WHERE artist=?"
    prt = f"{cur_artist}/{tot_artist}: {artist_sane} {fg.li_blue}"
    ins = "INSERT INTO artists VALUES(NULL, ?, 1)"
  if album:
    val = (parentid, album)
    sql = "SELECT status,id FROM albums WHERE artist_id=? AND album=?"
    prt = f"  {cur_artist}/{tot_artist}: {artist_sane} -- {cur_album}/{num_albums}: {album} {fg.li_blue}"
    ins = "INSERT INTO albums VALUES(NULL, ?, ?, 1)"
  if track:
    val = (parentid, track)
    sql = "SELECT status,id FROM tracks WHERE album_id=? AND track=?"
    prt = f"    {fg.li_blue}"
    ins = "INSERT INTO tracks VALUES(NULL, ?, ?, 1)"
  rs = db_fetch(sql, val)
  # ~ print(f"---rs = {rs}")
  if rs:
    try:
      s = statout[rs[0]]
    except:
      s = "OTHER"
    if rs[0]==status['FINISHED'] or \
       rs[0]==status['NOMETADATA'] and args.skip_tags or \
       rs[0]==status['LIVE'] and not args.live or \
       rs[0]==status['IGNORED']:
          print(prt+"FINISHED"+fg.rs) # output current progress and status
          return 0
    else:
      if not track:
        print(prt+s+fg.rs) # output current progress and status
      return rs[1]     
  else:
    if not track:
      print(prt+"START"+fg.rs) # output current progress and status
    c = db.cursor()
    c.execute(ins, val)
    return c.lastrowid
  
def grab_discography(search):
  global ytm, db, status
  global c, ca, cur_artist, tot_artist, cur_album, num_albums, artist_sane
  
  r = ytm.search(search, filter="artists")
  artist = r[0]["artist"]
  cur_artist = cur_artist + 1

  d = similar(search.lower(), artist.lower())
  e = similar(("the "+search).lower(), artist.lower())
  if d < 0.9 and e < 0.9: #not a good match
    print(f'best fit for "{search}" is "{artist}": not good enough to continue')
    write_error(f'BADARTIST: "{search}" best match is "{artist}"')
    return
  artist_id = r[0]["browseId"]
  artist_sane = sane_fn(artist)

  #check status in db 
  artist_dbid = db_check_status(artist=artist_sane)
  # ~ print(f"--- dbid: {artist_dbid}", end="")
  # ~ input(" == Press Enter to continue...")
  if not artist_dbid:
    return #next artist
  artist_status = status['FINISHED'] #unless any part later turns not not to be finished
  
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
  cur_album = 0
  num_albums = len(r)

  for a in r: #for each album in dicography
    ca = ca + 1
    cur_album = cur_album + 1
    album_id = a["browseId"]
    album_title = a["title"]
    album_sane = sane_fn(album_title)

    if not args.live: # see if album is live from title
      if is_live_album(album_sane):
        db.execute("INSERT INTO albums VALUES(NULL, ?, ?, ?)", (artist_dbid, album_sane, status['LIVE']))
        db.commit()
        print(f"  {cur_artist}/{tot_artist}: {artist_sane} -- {cur_album}/{num_albums}: {album_sane} {fg.li_blue}LIVE{fg.rs}")
        continue

    #check status in db  
    album_dbid = db_check_status(album=album_sane, parentid=artist_dbid)
    if not album_dbid:
      continue #next album
    album_status = status['FINISHED'] #unless any part later turns not not to be finished
    
    try:
      s = ytm.get_album(album_id)
    except requests.exceptions.ConnectionError:
      sys.exit("somehow mark this for retry")
    delay(1)
    tracks = len(s["tracks"])
    
    live = True
    if not args.live: # see if album is live from _track_names_
      for n in range(tracks):
        t = s["tracks"][n]      
        song_title = t["title"]
        song_sane = sane_fn(song_title)
        # ~ print(song_sane, is_live_album(song_sane))
        if not is_live_album(song_sane):
          live = False
          continue
      # ~ print(live)
      # ~ input(" == Press Enter to continue...")
      if live==True: 
        db.execute("UPDATE albums SET status=? WHERE id=?", (status['LIVE'], album_dbid))
        db.commit()
        print(f"\033[F  {cur_artist}/{tot_artist}: {artist_sane} -- {cur_album}/{num_albums}: {album_sane} {fg.li_blue}LIVE{fg.rs}     ")
        # this moves up one line and rewrites the "PRELOAD" from the #check status in db
        continue #skip to next album

    path = f"{args.output_dir}/{artist_sane}/{album_sane}/"
    os.makedirs(os.path.dirname(path), exist_ok=True)

    for n in range(tracks): #for each track in album
      t = s["tracks"][n]      
      song_title = t["title"]
      song_id = t["videoId"]
      song_sane = sane_fn(song_title)
      song_file = f"{n+1} - {song_sane}"

      #check status in db  
      track_dbid = db_check_status(track=song_sane, parentid=album_dbid)
      if not track_dbid:
        continue #next track
      downloaded = False

      if song_id:
        song_filename = f"{path}/{song_file}"
        if glob_exists(song_filename):
          track_status = status['INCOMPLETE']
          print(f"    {fg.li_blue}SKIPPED{fg.rs}", end="")
          
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
            print(f"    {fg.red}FAIL{fg.rs} - {song_file}", end="")
            write_error(f'FAIL: "{song_file}" was unable to download')
            track_status = status['INCOMPLETE']
            downloaded = True
          else:  
            print(f"    {fg.green}GOOD{fg.rs} - {song_file}", end="")
            track_status = status['NOMETADATA']
            c = c + 1
            downloaded = True
          if errors == 3:
            print("STOP == too many errors!")
            sys.exit()
            
        if not args.skip_tags:
          fn = glob_exists(song_filename)
          if fn:
            r = set_metadata(a, t, fn)
            if r:
              track_status = status['FINISHED']
        else:
          track_status = status['NOMETADATA']
        print("") # send newline
  
      else: #song_id = Null
        c = c + 1
        print(f"    {fg.red}NULL{fg.rs} - {song_file}")
        write_error(f'NULL: "{song_file}" had no song_id')
        track_status = status['PRELOAD']
        
      #finally track
      db.execute("UPDATE tracks SET status=? WHERE album_id=? AND id=?", (track_status, album_dbid, track_dbid))
      album_status = min(album_status, track_status)
      db.commit()
      if args.delay and downloaded:
        delay(40) # ===== DEBUG
    
    #finally album
    db.execute("UPDATE albums SET status=? WHERE artist_id=? AND id=?", (album_status, artist_dbid, album_dbid))
    artist_status = min(artist_status, album_status)
    db.commit()
  
  #finally artist  
  db.execute("UPDATE artists SET status=? WHERE id=?", (artist_status, artist_dbid))
  db.commit()
 

#=====main()
start = time.time()
parser = argparse.ArgumentParser(description='Download complete discographies from youtube music')
parser.add_argument('artists', metavar='ARTIST', type=str, nargs='*', help='artist to download')
parser.add_argument('-f', '--file', metavar='FILE', type=str, default='', help='load list of artists from file, one artist per line')
parser.add_argument('-o', '--output-dir', metavar='PATH', type=str, default='music', help='store discographies in specified directory')
parser.add_argument('-s', '--skip-albums', action='store_true', help='prompt which albums to skip')
parser.add_argument('-t', '--skip-tags', action='store_true', help=' skip saving music tags')
parser.add_argument('-d', '--delay', action='store_true', help='delay ~40s per album to avoid google ban')
parser.add_argument('-l', '--live', action='store_true', help='include live albums')
parser.add_argument('--no-database', action='store_true', help='do not use a database to reduce downloads')
parser.add_argument('--rescan', action='store_true', help='rescan all artists for missing metadata, newly avail songs')
args = parser.parse_args()

if args.output_dir[-1] == "/":
  args.output_dir = args.output_dir[0:-1]

# if no database, will auto set --rescan to populate
if not args.no_database:
  db = open_database()

if args.rescan:
  artists = os.listdir(args.output_dir)
else:
  artists = []
if args.file: # require file or artist(s)
  with open(args.file, "r") as f:
    artists = artists + f.read().split("\n")
elif args.artists:
  artists = artists + args.artists
if not artists:
  print("ERROR: it is required to have at least one artist or a --file artist list")
  sys.exit()
# refer to args.live, args.delay directly

if not os.path.exists("oauth.json"):
  print("cannot find oauth.json.  Please run\nytmusicapi oauth\non the command line to generate the file.")
  sys.exit()

write_error(f"\n===== {time.strftime('%Y-%m-%d %H:%M:%S', time.localtime())}\n") #timestamp file

ytm = YTMusic("oauth.json") #===== DEBUG need to except for bad oauth
c = 0 # track count total
ca = 0 # album count total

cur_artist = 0
tot_artist = len(artists)
artist_sane = ""
cur_album = 0
num_albums = 0

for artist in artists:
  if artist:
    grab_discography(artist)
        
db.close()
# print end stats
end = time.time()
elapsed = int(end - start)
hms = str(datetime.timedelta(seconds=(elapsed)))
per_hour = int((3600 / elapsed) * c) if elapsed != 0 else 0
print(f"=== {fg.li_blue}DONE{fg.rs} {ca} albums; {c} songs in {hms}; {per_hour} songs/hour")
