import os
from pathlib import Path
from collections import Counter
from argparse import ArgumentParser
import json
import re
from zipfile import ZipFile
from bia_integrator_tools.biostudies import load_submission, find_files_in_submission_file_lists, attributes_to_dict, File

# ToDo: Use environment variable for this
# Global GOOFYS base
GOOFYS_BASE = Path(f"{os.environ['HOME']}/temp/goofys/biostudies-pub/")
NFS_BASE = Path("/nfs/biostudies/.adm/databases/prod/submissions/")

#class FtypeSummary:
#    def __init__(self):
#        filetypes = {}
#
#    def add(self, ftype_details):
#        for ftype, details in ftype_details["filetype_breakdown"].items():
#            ftype2 = self.map_ftype(ftype)
#            if ftype2 not in filetypes:
#                filetypes[ftype2
#        

def _expand_path(original_path, accno, base=None):
    """Return full path to access file in a submission"""
    
    collection, number = re.findall("(S-B[A-Z]+)([0-9]+)", accno)[0]
    bases = []
    if base is not None:
        bases.append(Path(base))

    bases.extend([GOOFYS_BASE, NFS_BASE,])
    expanded_paths = [
        path_base / f"{collection}/{number}/{collection}{number}/Files/{original_path}"
        for path_base in bases
    ]
    # Add special case if accno between 0 and 99
    if collection == "S-BIAD" and int(number) <= 99:
        for path_base in (NFS_BASE, GOOFYS_BASE,):
            expanded_paths.append(path_base / f"S-BIAD/S-BIAD0-99/{accno}/Files/{original_path}")

    for expanded_path in expanded_paths:
        if Path(expanded_path).is_file():
            return expanded_path

    raise Exception(f"No file found for expanding {original_path} into {expanded_paths}")

def summarise_filetypes(filelist):
    """Create dictionary grouping entries in filelist by extension"""
    
    suffix_count = dict(Counter([ f.path.suffix for f in filelist ]))
    summary = { s: {"n": n, "total_size": 0} for s, n in suffix_count.items() }
    del suffix_count

    for f in filelist:
        summary[f.path.suffix]["total_size"] += f.size

    return summary

def flist_files_from_zip(zipfilepath, add_zip_to_suffix=False):
    """Get paths in a zipfile"""
    
    with ZipFile(zipfilepath) as zipfile:
        if add_zip_to_suffix:
            filelist = []
            for z in zipfile.infolist():
                # Next few lines to append 'zip' to suffix of file
                fpath = Path(z.filename)
                suffix = fpath.suffix
                if len(suffix) == 0:
                    suffix = ".zip_nosuffix"
                else:
                    suffix = ".zip_" + suffix[1:]
                zip_path = fpath.parent / (fpath.stem + suffix)
                filelist.append(File(path=zip_path, size=z.file_size))
        else:
            filelist = [File(path=z.filename, size=z.file_size) for z in zipfile.infolist()]
    return filelist

if __name__ == "__main__":
    parser = ArgumentParser("Summarise filetypes for studies")
    parser.add_argument("-i", "--input-path",
        help="Path to list of studies to query"
    )
    parser.add_argument("-a", "--accno",
        help="Accession number of study to query"
    )
    parser.add_argument("-o", "--output-path",
        help="Path to save results. If not provided use stdout"
    )

    args = parser.parse_args()
    if args.input_path is not None:
        accnos = [ l.strip() 
            for l in Path(args.input_path).read_text().split("\n")
            if len(l) > 0
        ]
    else:
        accnos = [args.accno,]
    
    # Get filelist for accnos
    summary_by_accno = {}
    for accno in accnos:
        try:
            submission = load_submission(accno)
            submission_attr_dict = attributes_to_dict(submission.attributes)
            study_section_attr_dict = attributes_to_dict(submission.section.attributes)
            title = submission_attr_dict.get("Title", None)
            if not title:
                title = study_section_attr_dict.get("Title", "Unknown")
            
            #for attribute in submission.attributes:
            #    if attribute.name == "Title":
            #        title = attribute.value
            #        break

            filelist = find_files_in_submission_file_lists(submission)
            filepaths = [str(f.path) for f in filelist]
            for filepath in filepaths:
                if filepath.endswith(".zip"):
                    expanded_path = _expand_path(filepath, accno)
                    filelist.extend(flist_files_from_zip(expanded_path, add_zip_to_suffix=True))
            del filepaths
            summary_by_accno[accno] = {
                "title": title,
                "filetypes": summarise_filetypes(filelist),
            }
            del filelist
        except Exception as e:
            print(f"There was an error for accno: {accno}. Error was: {e}")
            continue

    if args.output_path is None:
        print(json.dumps(summary_by_accno))
    else:
        Path(args.output_path).write_text(json.dumps(summary_by_accno))
