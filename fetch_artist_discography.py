# ~ /home/g3master/.local/bin

#!/bin/python3
#REQUIREMENTS: pip install ytmusicapi yt-dlp sanitize_filename sty music_tag
#requires apt install ffmpeg
#run ytmusicapi oauth to get oauth.json
#version 0.12

import json, sys, os, glob, subprocess, time, random, datetime, argparse, re, music_tag, sqlite3
from sanitize_filename import sanitize
from sty import fg, rs
from ytmusicapi import YTMusic
from difflib import SequenceMatcher
from change_fiber_ip import ChangeFiberIP

DAILY_LIMIT = 2500
BATCH_LIMIT = 500
DELAY_SONG  = 20
DELAY_ERROR = 1100

def count_file(check_only=False): # update daily song count in a file
  n = 0
  if not check_only:
    now = datetime.datetime.now()
    filename = now.strftime("%Y-%m-%d.cnt")
    try:
      f = open(filename, "r+")
      n = int(f.read()) or 0
    except FileNotFoundError:
      f = open(filename, "w")
      n = 0
    n = n + 1
    f.seek(0)
    f.write(str(n))
    f.close()
  if BATCH_LIMIT and c >= BATCH_LIMIT: # ===== BATCH LIMIT
    print(f"\n{fg.red}===== BATCH LIMIT REACHED: {BATCH_LIMIT} ====={fg.rs}")
    sys.exit()
  if DAILY_LIMIT and n >= DAILY_LIMIT: # ===== DAILY LIMIT
    print(f"\n{fg.red}===== DAILY LIMIT REACHED: {DAILY_LIMIT} ====={fg.rs}")
    sys.exit()

def count(db, check_only=False): # update daily song count in db
  now = datetime.datetime.now()
  today = int(now.strftime("%Y%m%d"))
  if not check_only:
    sql = f"INSERT INTO count VALUES({today}, 1) ON CONFLICT(date) DO UPDATE SET songs=songs+1;"
    db.executescript(sql)
    db.commit()
  try:
    n = db.execute(f"SELECT songs FROM count WHERE date={today}").fetchone()[0]
  except:
    n = 0
  if "c" in globals() and (BATCH_LIMIT and c > BATCH_LIMIT): # ===== BATCH LIMIT
    print(f"\n{fg.red}===== BATCH LIMIT REACHED: {BATCH_LIMIT} ====={fg.rs}")
    sys.exit()
  if DAILY_LIMIT and n >= DAILY_LIMIT: # ===== DAILY LIMIT
    print(f"\n{fg.red}===== DAILY LIMIT REACHED: {DAILY_LIMIT} ====={fg.rs}")
    sys.exit()

def dump_json(js, filename="temp.json"): # dump json_object as pretty json to file
  with open(filename, "w") as f:
    f.write(json.dumps(js, indent=2))

def sane_fn(f): #sanitize filename of illegal characters
  return sanitize(f.replace('/','-').replace('`',"'").replace("º","°"))

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
  skip_nums = re.compile(r'-?\d+').findall(skip_str)

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
    print(f" -- {fg.li_yellow}Metadata Incomplete{fg.rs}", end="")
    return False
  return True

def glob_exists(filename): # Detect song with any extension, return filename
  fn = glob.glob(glob.escape(filename)+".*")
  if fn:
    return fn[0]
  else:
    return False
  
def is_live_album(album_name): # Determines from title if album is live
  rgx = r"([\[\(]live[\]\)]|(live|bbc) (at|in|from|fm|bootleg|sessions|in concert|[1-2][0-9][0-9][0-9]|- )|^live! | live$|\(live-| live!|fm broadcast)"
  return re.search(rgx, album_name, re.I)

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
  db.execute("PRAGMA journal_mode=MEMORY")
  return db

def db_fetch(sql, values=None): # Get db result only, single row or scalar
  global db
  if type(values) is str:
    values = (values,) #needs to be tuple
  if values:
    r = db.execute(sql, values).fetchone()
  else:
    r = db.execute(sql).fetchone()
  if r and len(r)==1:
    return r[0]
  return r

def get_song(path, song_file, song_id):
  global c
  cmd = f'yt-dlp -q -x -P "{path}" -o "{song_file}" -- {song_id} '
  c = c + 1
  try:
    result = subprocess.run(cmd, shell=True, capture_output=True)
    # ~ print(f"return_code {return_code}")
  except KeyboardInterrupt:
    print()
    sys.exit()
  except e:
    print(f"{e.output}\nUNCAUGHT ERROR: {cmd}")
    dump_json(s)
    sys.exit()
  return result.returncode, result.stderr

status = {'PRELOAD': 1, 'NULL': 2, 'IGNORED': 3, 'LIVE': 4, 'NOMETADATA': 5, 'INCOMPLETE': 6, 'FINISHED': 9}
statout= {1: 'PRELOAD', 2: 'NULL', 3: 'IGNORED', 4: 'LIVE', 5: 'NOMETADATA', 6: 'INCOMPLETE', 9: 'FINISHED'}
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

  cur_artist = cur_artist + 1
  if use_db:
    s = db_fetch("SELECT status FROM artists WHERE artist=?", search)
    if s==status['FINISHED']:
      db.execute("UPDATE queue SET done=1 WHERE artist=?", (search,))
      print(f"{cur_artist}/{tot_artist}: {search} {fg.li_blue}FINISHED{fg.rs}")
      return
  
  print(f"search artist({search})")
  r = ytm.search(search, filter="artists")
  try:
    artist = r[0]["artist"]
  except IndexError:
    print(f'{fg.red}ERROR{fg.rs}: no match for "{search}"')
    db.execute("UPDATE queue SET done=1, suggest='BAD' WHERE artist=?", (search,))
    write_error(f'BADARTIST: "{search}" no matches"')
    return   

  d = similar(search.lower(), artist.lower())
  e = similar(("the "+search).lower(), artist.lower())
  if d < 0.9 and e < 0.9: #not a good match
    print(f'best fit for "{search}" is "{artist}": not good enough to continue')
    db.execute("UPDATE queue SET done=1, suggest=? WHERE artist=?", (artist, search))
    write_error(f'BADARTIST: "{search}" best match is "{artist}"')
    return
  artist_id = r[0]["browseId"]
  artist_sane = sane_fn(artist)

  if use_db:
    #check status in db 
    artist_dbid = db_check_status(artist=artist_sane)
    if not artist_dbid:
      return #next artist
    if args.preload:
      db.commit()
      return #skip to next album 
  else:
    print(f"{cur_artist}/{tot_artist}: {artist_sane} {fg.li_blue}CHECKING{fg.rs}")
  
  r = ytm.get_artist(artist_id)
  
  #===== DEBUG
  # ~ print(f"ytm.get_artist({artist_id})")
  # ~ print("r=",r)
  print("singles")
  try:
    for song in r["singles"]["results"]:
      print("-", song["title"], song["browseId"])
  except KeyError:
    print("No Singles!")
  # ~ print("videos")
  # ~ for song in r["videos"]["results"]:  
    # ~ print("-", song["title"], song["videoId"])
  # ~ sys.exit()
  
  try:
    discography_id = r["albums"]["browseId"]
  except KeyError:
    print(f"{fg.red}NO ALBUMS ERROR{fg.rs} ")
    write_error(f'BAD ALBUM: "{artist}" has no albums')
    return
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
  artist_status = status['FINISHED'] #unless any part later turns not not to be finished

  for a in r: #for each album in dicography
    ca = ca + 1
    cur_album = cur_album + 1
    album_id = a["browseId"]
    album_title = a["title"]
    album_sane = sane_fn(album_title)

    if not args.live: # see if album is live from title
      if is_live_album(album_sane):
        if use_db:
          db.execute("INSERT INTO albums VALUES(NULL, ?, ?, ?)", (artist_dbid, album_sane, status['LIVE']))
          db.commit()
        print(f"  {cur_artist}/{tot_artist}: {artist_sane} -- {cur_album}/{num_albums}: {album_sane} {fg.li_blue}LIVE{fg.rs}")
        continue

    if use_db:
      #check status in db  
      album_dbid = db_check_status(album=album_sane, parentid=artist_dbid)
      if not album_dbid:
        continue #next album
    else:
      print("{cur_artist}/{tot_artist}: {artist_sane} -- {cur_album}/{num_albums}: {album} {fg.li_blue}CHECKING{fg.rs}")

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
        if not is_live_album(song_sane):
          live = False
          continue
      if live==True: 
        if use_db:
          db.execute("UPDATE albums SET status=? WHERE id=?", (status['LIVE'], album_dbid))
          db.commit()
        print(f"\033[F  {cur_artist}/{tot_artist}: {artist_sane} -- {cur_album}/{num_albums}: {album_sane} {fg.li_blue}LIVE{fg.rs}     ")
        # this moves up one line and rewrites the "PRELOAD" from the #check status in db
        continue #skip to next album

    path = os.path.join(args.output_dir,artist_sane,album_sane)
    os.makedirs(os.path.dirname(path), exist_ok=True)
    
    for n in range(tracks): #for each track in album
      t = s["tracks"][n]      
      song_title = t["title"]
      song_id = t["videoId"]
      song_sane = sane_fn(song_title)
      song_file = f"{n+1} - {song_sane}"


      if use_db:
        #check status in db  
        track_dbid = db_check_status(track=song_sane, parentid=album_dbid)
        if not track_dbid:
          continue #next track

      downloaded = False
      if song_id:
        song_filename = os.path.join(path,song_file)
        if glob_exists(song_filename):
          track_status = status['INCOMPLETE']
          print(f"    {fg.li_blue}SKIPPED{fg.rs}", end="")
          
        else:
          return_code, stderror = get_song(path, song_file, song_id)
          # ~ print(return_code, stderror)
          
          if return_code == 1: # ===== handle various errors
            skip_error = False
            error_text = ""
            # errors to skip
            if b"Sign in to confirm your age" in stderror:
              skip_error = True
              error_text = "AGE ERROR"
            elif b"Signature extraction failed" in stderror or b"msig extraction failed" in stderror:
              skip_error = True
              error_text = "SIG EXTTRACTION ERROR"
            elif b"File name too long" in stderror:
              skip_error = True
              error_text = "FILENAME TOO LONG ERROR"
            elif b"The downloaded file is empty" in stderror:
              skip_error = True
              error_text = "DOWNLOADED FILE EMPTY"
            elif b"Join this channel to get access" in stderror:
              skip_error = True
              error_text = "SPECIAL CHANNEL ACCESS"
            elif b"Premieres in" in stderror:
              skip_error = True
              error_text = "SPECIAL CHANNEL ACCESS"

            elif b"Error 403: Forbidden" in stderror:
              error_text = "FORBIDDEN ERROR"
            elif b"Temporary failure in name resolution" in stderror:
              error_text = "NAME RESOLUTION ERROR"
            else:
              error_text = "OTHER ERROR\n"+str(stderror)
              
            if skip_error:
              errors = errors - 1 #don't count error for stoppage
            else:
              print(f"{fg.red}{error_text}{fg.rs} -- wait {DELAY_ERROR}s and try again")
              write_error(error_text)
              time.sleep(DELAY_ERROR)
              return_code, stderror = get_song(path, song_file, song_id)
              if return_code == 1:
                print(f"{fg.red}{error_text}{fg.rs} FAIL !!!")
                sys.exit()

          if return_code != 0: # ===== error have been sorted
            errors = errors + 1
            print(f"    {fg.red}FAIL{fg.rs} - {song_file} - {fg.red}{error_text}{fg.rs} ", end="")
            write_error(f'FAIL: "{song_file}" was unable to download')
            track_status = status['INCOMPLETE']
            downloaded = True
          else:  
            print(f"    {fg.green}GOOD{fg.rs} - {song_file}", end="")
            track_status = status['NOMETADATA']
            downloaded = True
            
            count(db) #=====DEBUG
            
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
        if downloaded:
          time.sleep(DELAY_SONG)
  
      else: #song_id = Null
        print(f"    {fg.red}NULL{fg.rs} - {song_file}")
        # ~ write_error(f'NULL: "{song_file}" had no song_id')
        track_status = status['NULL']
        
      #finally track
      if use_db:
        album_status = min(album_status, track_status)
        db.execute("UPDATE tracks SET status=? WHERE album_id=? AND id=?", (track_status, album_dbid, track_dbid))
        db.commit()
      if args.delay and downloaded and not skipped:
        delay(40) # ===== DEBUG
    
    #finally album
    if use_db:
      artist_status = min(artist_status, album_status)
      db.execute("UPDATE albums SET status=? WHERE artist_id=? AND id=?", (album_status, artist_dbid, album_dbid))
      db.commit()
  
  #finally artist
  if use_db:
    db.execute("UPDATE artists SET status=? WHERE id=?", (artist_status, artist_dbid))
    db.execute("UPDATE queue SET done=1 WHERE artist=?", (search,))
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
parser.add_argument('--preload', action='store_true', help='preload the artists for --daemon running in another shell')
parser.add_argument('--status', action='store_true', help='show current status of --daemon')
parser.add_argument('--daemon', action='store_true', help='act as server, waiting for requests and filling them, implies --delay')
parser.add_argument('--batch_limit', metavar='LIMIT', type=int, default=0, help='limit to download in one batch')
args = parser.parse_args()

if args.output_dir[-1] == "/":
  args.output_dir = args.output_dir[0:-1]

# if no database, will auto set --rescan to populate
use_db = not args.no_database
if use_db:
  db = open_database()
  count(db, check_only=True)
else:
  count_file(check_only=True)

if args.rescan:
  artists = os.listdir(args.output_dir)
else:
  artists = []
if args.file: # require file or artist(s)
  with open(args.file, "r") as f:
    artists = artists + f.read().split("\n")
  artists = list(filter(None, artists))
  if use_db: # load file into database, pull unfinsihed list from database
    for artist in artists:
      db.execute(f"INSERT OR IGNORE INTO queue (artist) VALUES (?)", (artist,))
    db.commit()
    artists = db_fetch("SELECT GROUP_CONCAT(artist, '|') FROM queue WHERE done=0").split("|")
  
elif args.artists:
  artists = artists + args.artists
if args.daemon:
  args.delay = True
  artists = db.execute("SELECT GROUP_CONCAT(artist,'|') FROM artists WHERE status=1").fetchone()[0].split("|")
if args.batch_limit:
  BATCH_LIMIT = args.batch_limit
print(f"batch_limit = {BATCH_LIMIT}")
if not artists:
  print("ERROR: it is required to have at least one artist or a --file artist list")
  sys.exit()
# refer to args.live, args.delay, other flags directly
# ===== END PARSE ARGUMENTS

# ~ if not os.path.exists("oauth.json") and not os.path.exists("auth.json"):
  # ~ print("cannot find oauth.json.  Please run\nytmusicapi oauth\non the command line to generate the file.")
  # ~ sys.exit()

write_error(f"\n===== {time.strftime('%Y-%m-%d %H:%M:%S', time.localtime())}\n") #timestamp file

ytm = YTMusic("auth.json") #===== DEBUG need to except for bad oauth
c = 0 # track count total
ca = 0 # album count total

cur_artist = 0
tot_artist = len(artists)
artist_sane = ""
cur_album = 0
num_albums = 0

if len(artists) > 5: #check for stale IP
  fiber = ChangeFiberIP("discography.sq3", "addresses")
  age = fiber.get_current_ip_age()
  if age > 2:
    fiber.change_ip()

for artist in artists:
  if artist:
    grab_discography(artist)

if use_db:        
  db.close()
# print end stats
end = time.time()
elapsed = int(end - start)
hms = str(datetime.timedelta(seconds=(elapsed)))
per_hour = int((3600 / elapsed) * c) if elapsed != 0 else 0
print(f"=== {fg.li_blue}DONE{fg.rs} {ca} albums; {c} songs in {hms}; {per_hour} songs/hour")
