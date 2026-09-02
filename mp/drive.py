"""Google Drive access: index the folder tree, then fetch files on demand.

Auth reuses the same installed-app OAuth client the GRN schedulers use. The
token belongs to marketing@thebakersdozen.in, which the "Market Place Data"
folder is shared with. Point GOOGLE_TOKEN_FILE / GOOGLE_CREDENTIALS_FILE at a
different pair to run as another account.

The tree is walked once and cached to index.json: 457 folders is ~460 API calls,
far too slow to repeat per run. `refresh=True` (CLI: --refresh-index) re-walks it
when new month folders appear.
"""

import io
import json
import logging
import os
import re
import time
from typing import Dict, List, Optional

from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from googleapiclient.http import MediaIoBaseDownload

from .sources import EXCLUDE_PATH, JUNK_NAMES, ROOT_FOLDER_ID, Source

logger = logging.getLogger("mp.drive")

HERE = os.path.dirname(os.path.abspath(__file__))
PROJECT = os.path.dirname(HERE)
INDEX_FILE = os.path.join(PROJECT, "index.json")
CACHE_DIR = os.environ.get("MP_CACHE_DIR", os.path.join(PROJECT, "cache"))

FOLDER_MIME = "application/vnd.google-apps.folder"
LIST_FIELDS = "nextPageToken, files(id,name,mimeType,size,modifiedTime)"


def _token_path() -> str:
    return os.environ.get(
        "GOOGLE_TOKEN_FILE",
        os.path.join(PROJECT, "token.json"))


#: Socket timeout for every Drive call, in seconds.
#:
#: Without this a download blocks forever if the connection dies mid-transfer -
#: which is exactly what a sleeping laptop does. One overnight run sat hung on a
#: single retry for seven hours after the network dropped, having transferred
#: nothing. A timeout turns that into a retryable error instead of a hang. It is
#: generous because the Instamart files run to ~350 MB each.
SOCKET_TIMEOUT = int(os.environ.get("MP_SOCKET_TIMEOUT", "600"))


def build_service():
    """An authorised Drive v3 client.

    The refreshed token is written back only if we own the file, so pointing at
    a scheduler's token.json never mutates it underneath that scheduler.
    """
    path = _token_path()
    if not os.path.exists(path):
        raise SystemExit(
            f"No Drive token at {path}. Copy a token.json with the "
            f"https://www.googleapis.com/auth/drive scope there, or set "
            f"GOOGLE_TOKEN_FILE.")
    creds = Credentials.from_authorized_user_file(path)
    if not creds.valid:
        creds.refresh(Request())
        if os.path.dirname(os.path.abspath(path)) == PROJECT:
            with open(path, "w", encoding="utf-8") as fh:
                fh.write(creds.to_json())

    import httplib2
    from google_auth_httplib2 import AuthorizedHttp

    http = AuthorizedHttp(creds, http=httplib2.Http(timeout=SOCKET_TIMEOUT))
    return build("drive", "v3", http=http, cache_discovery=False)


# --------------------------------------------------------------------------- #
# Index                                                                        #
# --------------------------------------------------------------------------- #

def _list_children(svc, folder_id: str) -> List[dict]:
    out: List[dict] = []
    page = None
    while True:
        for attempt in range(1, 4):
            try:
                resp = svc.files().list(
                    q=f"'{folder_id}' in parents and trashed = false",
                    fields=LIST_FIELDS, pageSize=1000, pageToken=page,
                    orderBy="folder,name",
                    supportsAllDrives=True, includeItemsFromAllDrives=True,
                ).execute()
                break
            except HttpError as exc:
                if attempt == 3:
                    raise
                logger.warning("list(%s) attempt %d failed: %s", folder_id, attempt, exc)
                time.sleep(2 * attempt)
        out.extend(resp.get("files", []))
        page = resp.get("nextPageToken")
        if not page:
            return out


def build_index(svc, root_id: str = ROOT_FOLDER_ID) -> dict:
    """Walk the whole tree once. Skips the excluded folder entirely."""
    files: List[dict] = []
    folders: List[dict] = []

    def walk(fid: str, path: str, depth: int):
        folders.append({"id": fid, "path": path, "depth": depth})
        for f in _list_children(svc, fid):
            child = f"{path}/{f['name']}"
            if f["mimeType"] == FOLDER_MIME:
                if f["name"] == EXCLUDE_PATH:
                    logger.info("[INDEX] skipping excluded folder: %s", child)
                    continue
                walk(f["id"], child, depth + 1)
            elif f["name"].lower() not in JUNK_NAMES:
                files.append({
                    "id": f["id"], "name": f["name"], "path": child,
                    "mime": f["mimeType"], "size": int(f.get("size") or 0),
                    "modified": f.get("modifiedTime", ""),
                })

    root = svc.files().get(fileId=root_id, fields="name",
                           supportsAllDrives=True).execute()
    logger.info("[INDEX] walking '%s' ...", root["name"])
    walk(root_id, root["name"], 0)
    index = {"root": root["name"], "root_id": root_id,
             "built_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
             "folders": folders, "files": files}
    with open(INDEX_FILE, "w", encoding="utf-8") as fh:
        json.dump(index, fh, indent=1)
    logger.info("[INDEX] %d folders, %d files -> %s",
                len(folders), len(files), INDEX_FILE)
    return index


def load_index(svc=None, refresh: bool = False) -> dict:
    if refresh or not os.path.exists(INDEX_FILE):
        return build_index(svc or build_service())
    with open(INDEX_FILE, encoding="utf-8") as fh:
        return json.load(fh)


# --------------------------------------------------------------------------- #
# Selecting a source's files                                                   #
# --------------------------------------------------------------------------- #

def _is_subsequence(folder_segments: List[str], path_segments: List[str]) -> bool:
    """Do the folder's segments appear in the path, in order?

    A plain prefix match is not enough. Back-history is filed under range-named
    folders that sometimes sit *between* the platform folder and the report
    folder, e.g.

        PLA/Feb-25 To Dec-25 Data/Monthly - Consolidated FSN report/...

    against a source folder of 'PLA/Monthly - Consolidated FSN report'. Matching
    the segments as an ordered subsequence picks those up while still keeping
    PCA and PLA (and each report type) strictly apart, because every segment
    still has to be present and in order.
    """
    it = iter(path_segments)
    return all(seg in it for seg in folder_segments)


def files_for(source: Source, index: dict) -> List[dict]:
    """Every data file belonging to `source`, oldest first."""
    root = index["root"]
    wanted = source.folder.split("/")
    out = []
    for f in index["files"]:
        if EXCLUDE_PATH in f["path"]:
            continue
        segments = f["path"].split("/")
        if not segments or segments[0] != root:
            continue
        directories = segments[1:-1]          # drop the root and the file name
        if not _is_subsequence(wanted, directories):
            continue
        relative = "/".join(segments[1:])
        if any(x in relative for x in source.exclude):
            continue
        out.append(f)
    return sorted(out, key=lambda f: (f["modified"], f["name"]))


# --------------------------------------------------------------------------- #
# Download                                                                     #
# --------------------------------------------------------------------------- #

def _safe(name: str) -> str:
    return re.sub(r"[^A-Za-z0-9._-]", "_", name)[:120]


def download(svc, meta: dict, cache_dir: str = CACHE_DIR) -> str:
    """Fetch a file to the local cache and return its path.

    Cached by Drive file id plus size, so a re-uploaded file with the same name
    is re-fetched rather than served stale.
    """
    os.makedirs(cache_dir, exist_ok=True)
    dest = os.path.join(cache_dir, f"{meta['id']}_{meta['size']}_{_safe(meta['name'])}")
    if os.path.exists(dest) and os.path.getsize(dest) > 0:
        return dest

    tmp = dest + ".part"
    attempts = int(os.environ.get("MP_DOWNLOAD_ATTEMPTS", "6"))
    for attempt in range(1, attempts + 1):
        try:
            request = svc.files().get_media(fileId=meta["id"])
            with io.FileIO(tmp, "wb") as fh:
                downloader = MediaIoBaseDownload(fh, request, chunksize=8 * 1024 * 1024)
                done = False
                while not done:
                    # num_retries makes the client retry individual chunks with
                    # its own backoff, so a brief blip does not discard the
                    # hundreds of MB already transferred.
                    _, done = downloader.next_chunk(num_retries=3)
            os.replace(tmp, dest)
            return dest
        except Exception as exc:  # noqa: BLE001
            if os.path.exists(tmp):
                os.remove(tmp)
            if attempt == attempts:
                raise
            # Long backoff on purpose: these failures are usually the network
            # being away (a suspended laptop, a dropped VPN), and retrying three
            # seconds later just burns the remaining attempts. Caps at ~2 min.
            delay = min(120, 5 * 2 ** (attempt - 1))
            logger.warning("download(%s) attempt %d/%d failed (%s); retrying in %ds",
                           meta["name"], attempt, attempts, str(exc)[:90], delay)
            time.sleep(delay)
    raise RuntimeError("unreachable")


#: When 0, a downloaded file is deleted as soon as it has been processed.
#: A GitHub runner has ~14 GB of disk and Instamart's monthly CSVs are ~350 MB
#: each, so keeping every download would fill it during a large catch-up.
#: Locally the default is to keep them, because re-running against a warm cache
#: is what makes verification passes cheap.
KEEP_CACHE = os.environ.get("MP_KEEP_CACHE", "1") != "0"


def discard(path: str) -> None:
    """Delete a cached download, unless the cache is being kept."""
    if KEEP_CACHE or not path:
        return
    try:
        if os.path.exists(path):
            os.remove(path)
    except OSError as exc:
        logger.debug("could not remove %s: %s", path, exc)


def purge_cache(cache_dir: str = CACHE_DIR) -> int:
    """Delete cached downloads. Returns bytes freed."""
    freed = 0
    if not os.path.isdir(cache_dir):
        return 0
    for name in os.listdir(cache_dir):
        p = os.path.join(cache_dir, name)
        if os.path.isfile(p):
            freed += os.path.getsize(p)
            os.remove(p)
    return freed
