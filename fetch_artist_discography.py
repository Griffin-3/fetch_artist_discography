# REQUIREMENTS: pip install ytmusicapi yt-dlp sanitize_filename sty music_tag paramiko requests
# requires apt install ffmpeg
# run ytmusicapi oauth to get oauth.json
# version 0.14

import json
import sys
import os
import glob
import time
import random
import datetime
import argparse
import re
import music_tag
import sqlite3
from typing import List, Dict, Optional, Tuple
from sanitize_filename import sanitize
from sty import fg, rs
from ytmusicapi import YTMusic
from difflib import SequenceMatcher
from change_fiber_ip import ChangeFiberIP

DAILY_LIMIT = 2500
BATCH_LIMIT = 550
DELAY_SONG = 20
DELAY_ERROR = 1100

class DiscographyDownloader:
    """Manages downloading and organizing music discographies from YouTube Music."""
    
    def __init__(self, args: argparse.Namespace):
        """Initialize downloader with arguments and setup database."""
        self.args = args
        self.ytm = YTMusic("auth.json")
        self.db = self._open_database() if not args.no_database else None
        self.count_total = 0  # Total tracks processed
        self.album_count = 0  # Total albums processed
        self.current_artist_idx = 0  # Current artist index
        self.total_artists = 0  # Total artists
        self.current_album_idx = 0  # Current album index
        self.total_albums = 0  # Total albums per artist
        self.artist_sane = ""  # Sanitized artist name
        self.status_codes = {
            'PRELOAD': 1, 'NULL': 2, 'IGNORED': 3, 'LIVE': 4,
            'NOMETADATA': 5, 'INCOMPLETE': 6, 'FINISHED': 9
        }
        self.status_names = {v: k for k, v in self.status_codes.items()}

    def _open_database(self) -> sqlite3.Connection:
        """Create or open SQLite database and initialize tables."""
        try:
            db = sqlite3.connect("discography.sq3")
            db.execute("PRAGMA journal_mode=MEMORY")
            if not db.execute(
                "SELECT EXISTS(SELECT 1 FROM sqlite_master WHERE type='table' AND name='artists')"
            ).fetchone()[0]:
                db.execute("CREATE TABLE artists (id INTEGER PRIMARY KEY, artist TEXT, status INTEGER)")
                db.execute("CREATE TABLE albums (id INTEGER PRIMARY KEY, artist_id INTEGER, album TEXT, status INTEGER)")
                db.execute("CREATE TABLE tracks (id INTEGER PRIMARY KEY, album_id INTEGER, track TEXT, status INTEGER)")
                db.execute("CREATE TABLE errors (code TEXT, message TEXT)")
                db.execute("CREATE TABLE queue (artist TEXT, done INTEGER DEFAULT 0, suggest TEXT)")
                db.execute("CREATE TABLE count (date INTEGER PRIMARY KEY, songs INTEGER)")
                db.commit()
                self.args.rescan = True
            return db
        except sqlite3.Error as e:
            self._send_telegram_alert(f"Database error: {e}")
            sys.exit(f"Database error: {e}")

    def _count_file(self, check_only: bool = False) -> int:
        """Update daily song count in a file."""
        now = datetime.datetime.now()
        filename = now.strftime("%Y-%m-%d.cnt")
        count = 0
        if not check_only:
            try:
                with open(filename, "r+") as f:
                    count = int(f.read() or 0)
                    count += 1
                    f.seek(0)
                    f.write(str(count))
            except FileNotFoundError:
                with open(filename, "w") as f:
                    f.write("1")
                count = 1
        if BATCH_LIMIT and self.count_total >= BATCH_LIMIT:
            print(f"\n{fg.red}===== BATCH LIMIT REACHED: {BATCH_LIMIT} ====={fg.rs}")
            sys.exit()
        if DAILY_LIMIT and count >= DAILY_LIMIT:
            print(f"\n{fg.red}===== DAILY LIMIT REACHED: {DAILY_LIMIT} ====={fg.rs}")
            sys.exit()
        return count

    def _count_db(self, check_only: bool = False) -> int:
        """Update daily song count in database."""
        now = datetime.datetime.now()
        today = int(now.strftime("%Y%m%d"))
        if not check_only:
            self.db.executescript(
                f"INSERT INTO count VALUES({today}, 1) ON CONFLICT(date) DO UPDATE SET songs=songs+1;"
            )
            self.db.commit()
        try:
            count = self.db.execute(f"SELECT songs FROM count WHERE date={today}").fetchone()[0]
        except (TypeError, IndexError):
            count = 0
        if BATCH_LIMIT and self.count_total >= BATCH_LIMIT:
            print(f"\n{fg.red}===== BATCH LIMIT REACHED: {BATCH_LIMIT} ====={fg.rs}")
            sys.exit()
        if DAILY_LIMIT and count >= DAILY_LIMIT:
            print(f"\n{fg.red}===== DAILY LIMIT REACHED: {DAILY_LIMIT} ====={fg.rs}")
            sys.exit()
        return count

    def _dump_json(self, data: Dict, filename: str = "temp.json") -> None:
        """Dump JSON data to a file with pretty-printing for debugging."""
        with open(filename, "w") as f:
            json.dump(data, f, indent=2, ensure_ascii=False)

    def _sane_filename(self, filename: str) -> str:
        """Sanitize filename by replacing illegal characters."""
        return sanitize(filename.replace('/', '-').replace('`', "'").replace("º", "°"))

    def _similarity(self, a: str, b: str) -> float:
        """Calculate similarity between two strings."""
        return SequenceMatcher(None, a, b).ratio()

    def _delay(self, seconds: int = 10) -> None:
        """Delay execution with random variation (±50%)."""
        time.sleep(random.randint(int(seconds / 2), int(seconds * 1.5)))

    def _send_telegram_alert(self, message: str) -> None:
        """Stub method to send a Telegram notification for unhandled errors."""
        # TODO: Implement Telegram notification using a library like python-telegram-bot
        print(f"TELEGRAM ALERT: {message} (Implement Telegram bot to send to user)")

    def _prompt_albums(self, albums: List[Dict]) -> List[Dict]:
        """Prompt user to select albums to skip."""
        print("Which albums should be skipped?")
        indexed_ids = {}
        for idx, album in enumerate(albums, 1):
            print(f"  {idx:2d} - {album['title']}")
            indexed_ids[idx] = album["browseId"]

        print(f"Enter numbers to skip separated by spaces (1-{len(albums)}):")
        print("(Enter 0 or leave blank to not skip any albums.)")
        skip_input = input().strip()
        skip_nums = [int(n) for n in re.findall(r'-?\d+', skip_input) if n.isdigit()]

        if not skip_nums or skip_nums[0] == 0:
            print("Not skipping any albums...")
            return albums

        for num in skip_nums:
            if num < 1 or num > len(albums):
                print(f"STOP == Invalid album number: {num}")
                sys.exit()

        skip_ids = [indexed_ids[num] for num in skip_nums]
        print(f"Skipping {len(skip_ids)} albums numbered: {' '.join(map(str, skip_nums))}...")
        return [album for album in albums if album["browseId"] not in skip_ids]

    def _set_metadata(self, album: Dict, track: Dict, filename: str) -> bool:
        """Set metadata for a downloaded track, returning True if album and artist are set."""
        try:
            tags = music_tag.load_file(filename)
        except NotImplementedError:
            return False
        if tags["album"]:
            return True  # Already tagged

        success = True
        incomplete_fields = []

        # Critical fields: album and artist
        try:
            tags["album"] = album["title"]
        except Exception as e:
            incomplete_fields.append(f"album: {e}")
            success = False

        try:
            tags["artist"] = track["artists"][0]["name"] if track.get("artists") and track["artists"][0].get("name") else ""
        except Exception as e:
            incomplete_fields.append(f"artist: {e}")
            success = False

        # Non-critical fields
        try:
            track_year = track.get("year", album.get("year", ""))
            tags["year"] = "" if track_year in ["Single", "EP"] else track_year
        except Exception as e:
            incomplete_fields.append("year")

        try:
            tags["tracktitle"] = track.get("title", "")
        except Exception as e:
            incomplete_fields.append("tracktitle")

        try:
            tags["tracknumber"] = track.get("trackNumber", 0)
        except Exception as e:
            incomplete_fields.append("tracknumber")

        if incomplete_fields:
            print(f" -- Metadata OK: no {', '.join(incomplete_fields)}", end="")
        else:
            print(" -- got metadata", end="")

        if success:
            tags.save()
        return success

    def _glob_exists(self, filename: str) -> Optional[str]:
        """Check if a file exists with any extension."""
        matches = glob.glob(glob.escape(filename) + ".*")
        return matches[0] if matches else None

    def _is_live_album(self, name: str) -> bool:
        """Determine if an album or track is live based on its name."""
        pattern = r"([\[\(]live[\]\)]|(live|bbc) (at|in|from|fm|bootleg|sessions|in concert|[1-2][0-9][0-9][0-9]|- )|^live! | live$|\(live-| live!|fm broadcast)"
        return bool(re.search(pattern, name, re.I))

    def _write_error(self, message: str) -> None:
        """Append error message to error.log."""
        with open("error.log", "a") as f:
            f.write(message + "\n")

    def _db_fetch(self, sql: str, values: Optional[Tuple] = None) -> Optional[any]:
        """Execute SQL query and return single result or scalar."""
        if isinstance(values, str):
            values = (values,)
        result = self.db.execute(sql, values or ()).fetchone()
        return result[0] if result and len(result) == 1 else result

    def _db_check_status(self, entity_type: str, name: str, parent_id: Optional[int] = None) -> int:
        """Check or insert status for artist, album, or track in database."""
        if entity_type == "artist":
            table, field, parent_field = "artists", "artist", None
            values = (name,)
            sql = "SELECT status, id FROM artists WHERE artist=?"
            insert_sql = "INSERT INTO artists VALUES(NULL, ?, 1)"
            display = f"{self.current_artist_idx}/{self.total_artists}: {self.artist_sane} {fg.li_blue}"
        elif entity_type == "album":
            table, field, parent_field = "albums", "album", "artist_id"
            values = (parent_id, name)
            sql = f"SELECT status, id FROM albums WHERE {parent_field}=? AND {field}=?"
            insert_sql = f"INSERT INTO albums VALUES(NULL, ?, ?, 1)"
            display = f"  {self.current_artist_idx}/{self.total_artists}: {self.artist_sane} -- {self.current_album_idx}/{self.total_albums}: {name} {fg.li_blue}"
        else:  # track
            table, field, parent_field = "tracks", "track", "album_id"
            values = (parent_id, name)
            sql = f"SELECT status, id FROM tracks WHERE {parent_field}=? AND {field}=?"
            insert_sql = f"INSERT INTO tracks VALUES(NULL, ?, ?, 1)"
            display = f"    {fg.li_blue}"

        result = self._db_fetch(sql, values)
        if result:
            status, entity_id = result
            status_name = self.status_names.get(status, "OTHER")
            if status in (
                self.status_codes['FINISHED'],
                self.status_codes['NOMETADATA'] if self.args.skip_tags else -1,
                self.status_codes['LIVE'] if not self.args.live else -1,
                self.status_codes['IGNORED']
            ):
                if entity_type != "track":
                    print(f"{display}FINISHED{fg.rs}")
                return 0
            if entity_type != "track":
                print(f"{display}{status_name}{fg.rs}")
            return entity_id
        else:
            if entity_type != "track":
                print(f"{display}START{fg.rs}")
            cursor = self.db.cursor()
            cursor.execute(insert_sql, values)
            self.db.commit()
            return cursor.lastrowid

    def _download_track(self, path: str, song_file: str, song_id: str) -> Tuple[int, str]:
        """Download a track using yt-dlp."""
        import yt_dlp
        os.makedirs(path, exist_ok=True)
        output_template = os.path.join(path, f"{song_file}.%(ext)s")
        ydl_opts = {
            'format': 'bestaudio/best',
            'extractaudio': True,
            'outtmpl': output_template,
            'quiet': True,
            'no_warnings': False,
            'postprocessors': [{
                'key': 'FFmpegExtractAudio',
                'preferredcodec': 'opus',
                'preferredquality': '0',  # 0 ensures the best quality for opus
            }],
        }

        self.count_total += 1
        try:
            with yt_dlp.YoutubeDL(ydl_opts) as ydl:
                ydl.download([song_id])
            return 0, ""
        except yt_dlp.DownloadError as e:
            return 1, str(e)
        except KeyboardInterrupt:
            print()
            sys.exit()
        except Exception as e:
            error_msg = f"UNCAUGHT ERROR: {song_id}\n{e}"
            self._send_telegram_alert(error_msg)
            sys.exit(error_msg)

    def grab_track(self, album_data: Dict, track_data: Dict, album_path: str, album_db_id: int) -> int:
        """Process a single track."""
        song_title = track_data["title"]
        song_id = track_data.get("videoId")
        song_sane = self._sane_filename(song_title)
        # Use trackNumber if not None; otherwise, omit prefix
        track_number = track_data.get("trackNumber")
        song_file = f"{track_number} - {song_sane}" if track_number is not None else song_sane
        track_status = self.status_codes['INCOMPLETE']

        if self.db:
            track_db_id = self._db_check_status("track", song_sane, album_db_id)
            if not track_db_id:
                return self.status_codes['FINISHED']

        skip_delay = False
        song_filename = os.path.join(album_path, song_file)
        if existing_file := self._glob_exists(song_filename):
            print(f"    {fg.li_blue}SKIPPED{fg.rs}", end="")
            skip_delay = True

        elif song_id:
            return_code, stderr = self._download_track(album_path, song_file, song_id)
            skip_error = False
            error_text = ""
            errors = 0  # Track download errors for this track

            if return_code == 1:
                error_patterns = {
                    "Sign in to confirm your age": "AGE ERROR",
                    "Signature extraction failed|msig extraction failed": "SIG EXTRACTION ERROR",
                    "File name too long": "FILENAME TOO LONG ERROR",
                    "The downloaded file is empty": "DOWNLOADED FILE EMPTY",
                    "Join this channel to get access": "SPECIAL CHANNEL ACCESS",
                    "Premieres in": "SPECIAL CHANNEL ACCESS",
                    "Error 403: Forbidden": "FORBIDDEN ERROR",
                    "Temporary failure in name resolution": "NAME RESOLUTION ERROR",
                }
                for pattern, message in error_patterns.items():
                    if re.search(pattern, stderr, re.I):
                        skip_error = message in [
                            "AGE ERROR", "SIG EXTRACTION ERROR", "FILENAME TOO LONG ERROR",
                            "DOWNLOADED FILE EMPTY", "SPECIAL CHANNEL ACCESS"
                        ]
                        error_text = message
                        break
                else:
                    error_text = f"OTHER ERROR\n{stderr}"
                    self._send_telegram_alert(f"Unhandled yt-dlp error for {song_file}: {error_text}")

                if not skip_error:
                    print(f"{fg.red}{error_text}{fg.rs} -- wait {DELAY_ERROR}s and try again")
                    self._write_error(error_text)
                    self._delay(DELAY_ERROR)
                    return_code, stderr = self._download_track(album_path, song_file, song_id)
                    if return_code == 1:
                        print(f"{fg.red}{error_text}{fg.rs} FAIL !!!")
                        errors += 1
                        self._send_telegram_alert(f"Persistent yt-dlp error for {song_file}: {error_text}")
                        sys.exit()

                if errors >= 3:
                    error_msg = "STOP == too many errors!"
                    self._send_telegram_alert(error_msg)
                    print(error_msg)
                    sys.exit()

            if return_code != 0:
                print(f"    {fg.red}FAIL{fg.rs} - {song_file} - {fg.red}{error_text}{fg.rs}", end="")
                self._write_error(f'FAIL: "{song_file}" was unable to download')
                track_status = self.status_codes['INCOMPLETE']
            else:
                # Adjust output to omit track number if None
                display_file = song_sane if track_number is None else f"{track_number} - {song_sane}"
                print(f"    {fg.green}GOOD{fg.rs} - {display_file}", end="")
                track_status = self.status_codes['NOMETADATA']
                self._count_db() if self.db else self._count_file()

            if not self.args.skip_tags and (existing_file := self._glob_exists(song_filename)):
                if self._set_metadata(album_data, track_data, existing_file):
                    track_status = self.status_codes['FINISHED']
        else:
            # Adjust output for NULL case
            display_file = song_sane if track_number is None else f"{track_number} - {song_sane}"
            print(f"    {fg.red}NULL{fg.rs} - {display_file}", end="")
            track_status = self.status_codes['NULL']

        print("")  # Newline after track processing
        if self.db:
            self.db.execute("UPDATE tracks SET status=? WHERE album_id=? AND id=?", 
                           (track_status, album_db_id, track_db_id))
            self.db.commit()
        if not skip_delay:
            self._delay(DELAY_SONG)         
        return track_status

    def grab_album(self, album_data: Dict, artist_db_id: int, artist_name_sane: str) -> int:
        """Process a single album."""
        self.current_album_idx += 1
        self.album_count += 1
        album_id = album_data["browseId"]
        album_title = album_data["title"]
        album_sane = self._sane_filename(album_title)

        if not self.args.live and self._is_live_album(album_sane):
            if self.db:
                self.db.execute("INSERT INTO albums VALUES(NULL, ?, ?, ?)", 
                               (artist_db_id, album_sane, self.status_codes['LIVE']))
                self.db.commit()
            print(f"  {self.current_artist_idx}/{self.total_artists}: {artist_name_sane} -- "
                  f"{self.current_album_idx}/{self.total_albums}: {album_sane} {fg.li_blue}LIVE{fg.rs}")
            return self.status_codes['LIVE']

        if self.db:
            album_db_id = self._db_check_status("album", album_sane, artist_db_id)
            if not album_db_id:
                return self.status_codes['FINISHED']

        try:
            album_info = self.ytm.get_album(album_id)
        except Exception as e:
            error_msg = f"Failed to fetch album {album_sane}: {e}"
            self._send_telegram_alert(error_msg)
            self._write_error(f"ALBUM FETCH ERROR: {album_sane} - {str(e)}")
            sys.exit("Mark for retry not implemented")

        album_path = os.path.join(self.args.output_dir, artist_name_sane, album_sane)
        album_status = self.status_codes['FINISHED']

        is_live = all(self._is_live_album(self._sane_filename(track["title"])) for track in album_info["tracks"])
        if not self.args.live and is_live:
            if self.db:
                self.db.execute("UPDATE albums SET status=? WHERE id=?", 
                               (self.status_codes['LIVE'], album_db_id))
                self.db.commit()
            print(f"\033[F  {self.current_artist_idx}/{self.total_artists}: {artist_name_sane} -- "
                  f"{self.current_album_idx}/{self.total_albums}: {album_sane} {fg.li_blue}LIVE{fg.rs}     ")
            return self.status_codes['LIVE']

        for track_data in album_info["tracks"]:
            track_status = self.grab_track(album_data, track_data, album_path, album_db_id)
            album_status = min(album_status, track_status)

        if self.db:
            self.db.execute("UPDATE albums SET status=? WHERE artist_id=? AND id=?", 
                           (album_status, artist_db_id, album_db_id))
            self.db.commit()
        return album_status

    def parse_albums(self, artist_info: Dict, artist_match: str, artist_db_id: int) -> List[Dict]:
        """Parse albums, EPs, singles, and playlist-based albums from artist_info into a unified album list."""
        albums = []
        
        # Handle regular albums
        try:
            albums.extend(artist_info.get("albums", {}).get("results", []))
            discography_id = artist_info["albums"].get("browseId")
            discography_params = artist_info["albums"].get("params")
            if discography_params:
                albums = self.ytm.get_artist_albums(discography_id, discography_params)
        except KeyError:
            pass  # No albums, continue to singles/EPs

        # Handle singles and EPs
        single_tracks = []
        ep_albums = []
        try:
            singles = artist_info.get("singles", {}).get("results", [])
            songs = artist_info.get("songs", {}).get("results", [])
            
            # Separate singles and EPs
            for single in singles:
                if single.get("year") == "EP" and single.get("browseId"):
                    ep_albums.append({
                        "title": single.get("title", ""),
                        "browseId": single.get("browseId"),
                        "year": single.get("year")
                    })
                else:
                    single_tracks.append(single)

            # Add EPs as albums
            albums.extend(ep_albums)

            # Create virtual "Singles" album
            if single_tracks:
                virtual_album = {
                    "title": "Singles",
                    "browseId": None,
                    "year": None,
                    "tracks": [
                        {
                            "title": single.get("title", ""),
                            "videoId": next((song.get("videoId") for song in songs 
                                            if song.get("album", {}).get("id") == single.get("browseId")), None),
                            "trackNumber": None,  # Singles keep None to avoid numbering
                            "artists": [{"name": artist_match}],
                            "year": single.get("year")
                        } for single in single_tracks
                    ]
                }
                albums.append(virtual_album)
        except KeyError:
            pass  # No singles/EPs, continue to playlist-based albums

        # Handle playlist-based albums
        processed_albums = []
        for album in albums:
            if album.get("audioPlaylistId"):
                try:
                    # Fetch playlist tracks
                    playlist = self.ytm.get_playlist(album["audioPlaylistId"])
                    tracks = [
                        {
                            "title": track.get("title", ""),
                            "videoId": track.get("videoId"),
                            "trackNumber": index + 1,  # Assign sequential track number (1-based)
                            "artists": track.get("artists", [{"name": artist_match}]),
                            "year": album.get("type")
                        } for index, track in enumerate(playlist.get("tracks", []))
                    ]
                    # Print yt-dlp commands and metadata for playlist-based albums
                    album_sane = self._sane_filename(album["title"])
                    album_path = os.path.join(self.args.output_dir, self._sane_filename(artist_match), album_sane)
                    ##### ~ print(f"\nProcessing playlist-based album: {album['title']}")
                    for track in tracks:
                        song_sane = self._sane_filename(track["title"])
                        song_file = f"{track['trackNumber']} - {song_sane}" if track["trackNumber"] else song_sane
                        song_filename = os.path.join(album_path, f"{song_file}.opus")
                        ##### ~ print(f"Would download with yt-dlp: {track['videoId']} to {song_filename}")
                        ##### ~ print(f"Metadata: album={album['title']}, artist={track['artists'][0]['name']}, tracktitle={track['title']}, tracknumber={track['trackNumber']}, year={track['year']}")
                    # Add pseudo-album with tracks for processing
                    processed_albums.append({
                        "title": album["title"],
                        "browseId": album.get("browseId"),
                        "year": album.get("type"),
                        "tracks": tracks
                    })
                except Exception as e:
                    print(f"Failed to fetch playlist {album['audioPlaylistId']}: {e}")
                    self._write_error(f"PLAYLIST FETCH ERROR: {album['title']} - {str(e)}")
                    continue
            else:
                # Regular album (no playlist)
                processed_albums.append(album)

        return processed_albums
    
    def grab_discography(self, artist_name: str) -> None:
        """Process an artist's discography."""
        self.current_artist_idx += 1
        self.artist_sane = self._sane_filename(artist_name)

        if self.db:
            status = self._db_fetch("SELECT status FROM artists WHERE artist=?", artist_name)
            if status == self.status_codes['FINISHED']:
                self.db.execute("UPDATE queue SET done=1 WHERE artist=?", (artist_name,))
                print(f"{self.current_artist_idx}/{self.total_artists}: {artist_name} {fg.li_blue}FINISHED{fg.rs}")
                return

        try:
            search_results = self.ytm.search(artist_name, filter="artists")
        except Exception as e:
            error_msg = f"Failed to search artist {artist_name}: {e}"
            self._send_telegram_alert(error_msg)
            self._write_error(error_msg)
            return

        try:
            artist_info = search_results[0]
            artist_match = artist_info["artist"]
        except (IndexError, KeyError):
            error_msg = f"ERROR: No match for '{artist_name}'"
            print(f"{fg.red}{error_msg}{fg.rs}")
            self._write_error(f'BADARTIST: "{artist_name}" no matches')
            if self.db:
                self.db.execute("UPDATE queue SET done=1, suggest='BAD' WHERE artist=?", (artist_name,))
                self.db.commit()
            return

        similarity = max(
            self._similarity(artist_name.lower(), artist_match.lower()),
            self._similarity(f"the {artist_name}".lower(), artist_match.lower())
        )
        if similarity < 0.9:
            error_msg = f"Best fit for '{artist_name}' is '{artist_match}': not good enough to continue"
            print(error_msg)
            self._write_error(f'BADARTIST: "{artist_name}" best match is "{artist_match}"')
            if self.db:
                self.db.execute("UPDATE queue SET done=1, suggest=? WHERE artist=?", (artist_match, artist_name))
                self.db.commit()
            return

        artist_id = artist_info["browseId"]
        self.artist_sane = self._sane_filename(artist_match)

        if self.db:
            artist_db_id = self._db_check_status("artist", self.artist_sane)
            if not artist_db_id:
                return
            if self.args.preload:
                self.db.commit()
                return

        try:
            artist_info = self.ytm.get_artist(artist_id)
        except Exception as e:
            error_msg = f"Failed to fetch artist {artist_name}: {e}"
            self._send_telegram_alert(error_msg)
            self._write_error(error_msg)
            return

        # Parse albums, EPs, and singles
        albums = self.parse_albums(artist_info, artist_match, artist_db_id)
        if not albums:
            error_msg = f"NO ALBUMS ERROR for '{artist_match}'"
            print(f"{fg.red}{error_msg}{fg.rs}")
            self._write_error(f'BAD ALBUM: "{artist_match}" has no albums')
            self._dump_json(artist_info, self.artist_sane + ".json")
            if self.db:
                self.db.execute("UPDATE queue SET done=1 WHERE artist=?", (artist_name,))
                self.db.commit()
            return

        if self.args.skip_albums:
            albums = self._prompt_albums(albums)

        self.current_album_idx = 0
        self.total_albums = len(albums)
        artist_status = self.status_codes['FINISHED']

        # Process albums (regular, EPs, and virtual Singles)
        for album_data in albums:
            if album_data["title"] == "Singles" and album_data["browseId"] is None:
                # Handle virtual Singles album separately
                if self.db:
                    album_db_id = self._db_check_status("album", "Singles", artist_db_id)
                    if not album_db_id:
                        continue
                album_path = os.path.join(self.args.output_dir, self.artist_sane, "Singles")
                album_status = self.status_codes['FINISHED']
                for track_data in album_data.get("tracks", []):
                    track_status = self.grab_track(album_data, track_data, album_path, album_db_id)
                    album_status = min(album_status, track_status)
                if self.db:
                    self.db.execute("UPDATE albums SET status=? WHERE artist_id=? AND id=?", 
                                    (album_status, artist_db_id, album_db_id))
                    self.db.commit()
            else:
                # Regular albums and EPs
                album_status = self.grab_album(album_data, artist_db_id, self.artist_sane)
            artist_status = min(artist_status, album_status)

        if self.db:
            self.db.execute("UPDATE artists SET status=? WHERE id=?", (artist_status, artist_db_id))
            self.db.execute("UPDATE queue SET done=1 WHERE artist=?", (artist_name,))
            self.db.commit()

    def run(self, artists: List[str]) -> None:
        """Run the discography downloader for a list of artists."""
        start = time.time()
        self.total_artists = len(artists)

        if len(artists) > 5:
            fiber = ChangeFiberIP("discography.sq3", "addresses")
            if fiber.get_current_ip_age() > 2:
                fiber.change_ip()

        self._write_error(f"\n===== {time.strftime('%Y-%m-%d %H:%M:%S', time.localtime())} =====")
        for artist in artists:
            if artist:
                self.grab_discography(artist)

        if self.db:
            self.db.close()
        end = time.time()
        elapsed = int(end - start)
        hms = str(datetime.timedelta(seconds=elapsed))
        per_hour = int((3600 / elapsed) * self.count_total) if elapsed else 0
        print(f"=== {fg.li_blue}DONE{fg.rs} {self.album_count} albums; {self.count_total} tracks in {hms}; {per_hour} tracks/hour")

def main():
    """Parse arguments and start the downloader."""
    parser = argparse.ArgumentParser(description='Download complete discographies from YouTube Music')
    parser.add_argument('artists', metavar='ARTIST', type=str, nargs='*', help='artist to download')
    parser.add_argument('-f', '--file', metavar='FILE', type=str, default='', help='load list of artists from file')
    parser.add_argument('-o', '--output-dir', metavar='PATH', type=str, default='music', help='store discographies')
    parser.add_argument('-s', '--skip-albums', action='store_true', help='prompt which albums to skip')
    parser.add_argument('-t', '--skip-tags', action='store_true', help='skip saving music tags')
    parser.add_argument('-d', '--delay', action='store_true', help='delay ~40s per album to avoid ban')
    parser.add_argument('-l', '--live', action='store_true', help='include live albums')
    parser.add_argument('--no-database', action='store_true', help='do not use database')
    parser.add_argument('--rescan', action='store_true', help='rescan for missing metadata or songs')
    parser.add_argument('--preload', action='store_true', help='preload artists for daemon')
    parser.add_argument('--status', action='store_true', help='show daemon status')
    parser.add_argument('--daemon', action='store_true', help='run as daemon, implies --delay')
    parser.add_argument('--batch_limit', metavar='LIMIT', type=int, default=0, help='limit per batch')
    args = parser.parse_args()

    if args.output_dir.endswith("/"):
        args.output_dir = args.output_dir[:-1]
    if args.daemon:
        args.delay = True

    artists = []
    if args.rescan:
        artists = os.listdir(args.output_dir)
    if args.file:
        with open(args.file, "r") as f:
            artists.extend(line.strip() for line in f if line.strip())
    artists.extend(args.artists)
    if args.daemon and not args.no_database:
        db = sqlite3.connect("discography.sq3")
        artists = db.execute("SELECT GROUP_CONCAT(artist,'|') FROM queue WHERE done=0").fetchone()[0].split("|")
        db.close()
    if args.batch_limit:
        global BATCH_LIMIT
        BATCH_LIMIT = args.batch_limit
    if not artists:
        print("ERROR: At least one artist or a --file artist list is required, none left in queue.")
        sys.exit()

    downloader = DiscographyDownloader(args)
    if not args.no_database:
        downloader._count_db(check_only=True)
    else:
        downloader._count_file(check_only=True)
    downloader.run(artists)

if __name__ == "__main__":
    main()
