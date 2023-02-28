import hashlib
import logging
from urllib.parse import urlparse
from pathlib import Path
#from zipfile import ZipFile
import re
from remotezip import RemoteZip, RemoteIOError

import click

from bia_integrator_core.integrator import load_and_annotate_study
from bia_integrator_core.interface import persist_study
from bia_integrator_core.models import BIAFile, BIAImageRepresentation, BIAImage
from bia_integrator_tools.io import copy_uri_to_local

from bst_pulldown import IMAGE_EXTS

FIRE_FTP_ENDPOINT = "https://ftp.ebi.ac.uk/biostudies/fire"
logger = logging.getLogger(__file__)


def fetch_zipfile_with_caching(zipfile: BIAFile) -> Path:

    # FIXME - not always first representation
    zipfile_rep = zipfile.representations[0]
    cache_dirpath = Path.home()/".cache"/"bia-converter"
    cache_dirpath.mkdir(exist_ok=True, parents=True)

    suffix = Path(urlparse(zipfile_rep.uri).path).suffix
    prefix = hashlib.md5(zipfile_rep.uri.encode()).hexdigest()
    dst_fname = prefix + suffix
    dst_fpath = cache_dirpath/dst_fname
    
    if not dst_fpath.exists():
        copy_uri_to_local(zipfile_rep.uri, dst_fpath)
        logger.info(f"Downloading zipfile to {dst_fpath}")
    else:
        logger.info(f"Zipfile exists at {dst_fpath}")

    return dst_fpath
    

@click.command()
@click.argument('accession_id')
@click.argument('zipfile_id')
def main(accession_id, zipfile_id):

    logging.basicConfig(level=logging.INFO)

    bia_study = load_and_annotate_study(accession_id)

    zipfile = bia_study.archive_files[zipfile_id]
    collection, number = re.findall("(S-B[A-Z]+)([0-9]+)", accession_id)[0]
    assert len(collection) > 0
    assert type(int(number)) is int

    #zipfile_fpath = fetch_zipfile_with_caching(zipfile)
    zipfile_url = [
        # Try FIRE_FTP_ENPOINT 1st as that to biostudies API does not work
        # well with range requests (required for getting only portion of
        # zip with content info
        f"{FIRE_FTP_ENDPOINT}/{collection}/{number}/{accession_id}/Files/{zipfile.original_relpath}",
        zipfile.representations[0].uri,
    ]

    info_list = None
    for url in zipfile_url:
        try:
            with RemoteZip(url) as zipf:
                info_list = zipf.infolist()
            break
        except RemoteIOError as e:
            logging.info(f"{e}")
            continue
    if info_list is None:
        raise Exception(f"Could not access zipfile in following urls {zipfile_url}")

    image_zipinfos = [
        zipinfo for zipinfo in info_list
        if Path(zipinfo.filename).suffix in IMAGE_EXTS
    ]

    images = {}
    for n, zipinfo in enumerate(image_zipinfos, start=1):

        image_id = f"{zipfile_id}-IM{n}"

        rep = BIAImageRepresentation(
            accession_id=accession_id,
            image_id=image_id,
            # FIXME
            uri=zipfile.representations[0].uri,
            size=zipinfo.file_size,
            type="zipfile",
            attributes = {"zip_filename": zipinfo.filename}
        )

        images[image_id] = BIAImage(
            id=image_id,
            original_relpath=zipinfo.filename,
            representations=[rep]
        )

    bia_study.images.update(images)
    persist_study(bia_study)

    


if __name__ == "__main__":
    main()
